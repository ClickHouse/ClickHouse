#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/SettingsTierType.h>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Common/Documentation.h>
#include <Common/IFactoryWithAliases.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <functional>
#include <memory>
#include <optional>
#include <source_location>
#include <utility>

#include <boost/noncopyable.hpp>

namespace DB
{

static constexpr auto DEFAULT_CODEC_NAME = "Default";

class ICompressionCodec;
class IDataType;
struct Settings;
using DataTypePtr = std::shared_ptr<const IDataType>;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

using CodecNameWithLevel = std::pair<String, std::optional<int>>;

struct CodecValidationSettings
{
    explicit CodecValidationSettings(const Settings & settings_)
        : settings(&settings_)
    {
    }

    /// The stored pointer would dangle when constructed from a temporary.
    explicit CodecValidationSettings(Settings &&) = delete;

    /// An already accepted codec must not be re-judged by the current session, or existing tables could fail to load.
    static CodecValidationSettings trusted() { return {}; }

    /// Enforce the codec gate, but skip the suspicious-codec sanity checks. For the escape
    /// hatches that only relax validation (e.g. `allow_suspicious_ttl_expressions`): relaxing the checks
    /// must not silently enable a gated codec.
    static CodecValidationSettings withoutSanityCheck(const Settings & settings_)
    {
        CodecValidationSettings result(settings_);
        result.skip_sanity_check = true;
        return result;
    }

    static CodecValidationSettings withoutSanityCheck(Settings &&) = delete;

    /// nullptr on trusted paths (every gated / suspicious codec is accepted).
    /// Otherwise a gated codec must be enabled by its dedicated setting.
    const Settings * settings = nullptr;

    /// Skip the suspicious-codec sanity checks even though `settings` is set. See `withoutSanityCheck`.
    bool skip_sanity_check = false;

private:
    CodecValidationSettings() = default;
};

/** Creates a codec object by name of compression algorithm family and parameters.
 */
class CompressionCodecFactory final : private boost::noncopyable
{
protected:
    using Creator = std::function<CompressionCodecPtr(const ASTPtr & parameters)>;
    using CreatorWithType = std::function<CompressionCodecPtr(const ASTPtr & parameters, const IDataType * column_type)>;
    using SimpleCreator = std::function<CompressionCodecPtr()>;

    using CompressionCodecsDictionary = UnorderedMapWithMemoryTracking<String, CreatorWithType>;
    using CompressionCodecsCodeDictionary = UnorderedMapWithMemoryTracking<uint8_t, CreatorWithType>;

public:
    static CompressionCodecFactory & instance();

    /// Return default codec (currently LZ4)
    CompressionCodecPtr getDefaultCodec() const;

    /// True if `codec` is the default codec: no CODEC clause (null), or a lone CODEC(Default).
    /// A compound such as CODEC(Delta, Default) would return false.
    static bool isDefaultCodec(const ASTPtr & codec);

    /// Validate codecs AST specified by user and parses codecs description (substitute default parameters)
    ASTPtr validateCodecAndGetPreprocessedAST(
        const ASTPtr & ast, const DataTypePtr & column_type, const CodecValidationSettings & validation_settings) const;

    /// Validate codecs AST specified by user
    void validateCodec(const String & family_name, std::optional<int> level, const CodecValidationSettings & validation_settings) const;

    /// Validate a full codec expression given as a string, e.g. "ZSTD(3)" or "Delta, LZ4", without a column
    /// data type. This is the form stored in the codec-valued MergeTree settings (`marks_compression_codec`,
    /// `primary_key_compression_codec`, `default_compression_codec`). The suspicious-codec sanity checks do not apply to this form.
    void validateCodecString(const String & compression_codec, const CodecValidationSettings & validation_settings) const;

    /// Throw if `compression_codec` can never work on data whose column type is unknown, i.e. if
    /// `getReasonUnsafeForUntypedData` classifies it as unsafe. `validateCodecString` alone is not enough
    /// for the codec-valued MergeTree settings: it validates with the sanity checks disabled, so it rejects
    /// a codec that requires the column type (`T64`) but accepts a lossy one (`SZ3`), which would then only
    /// fail later, at the first mark / primary key / part write. `setting_name` is only used in the message.
    void checkCodecStringSafeForUntypedData(const String & compression_codec, std::string_view setting_name) const;

    /// Whether the codec family `family_name` is gated behind a dedicated `enable_<family>_codec`
    /// setting, i.e. it is not generally available. An unknown or ungated family is not gated.
    static bool isCodecFamilyGated(const String & family_name);

    /// Whether any codec family in the chain `compression_codec` (a codec name or chain such as
    /// `"PCO, LZ4"`) is gated. Classifies instead of throwing: a string that cannot be parsed as a
    /// codec chain is reported as ungated and fails later, where the codec is actually resolved.
    static bool isCodecStringGated(const String & compression_codec);

    /// Whether `settings` satisfy the gate of every codec in the chain `compression_codec`: each gated
    /// codec's dedicated `enable_<family>_codec` setting, or - for an experimental one - the blanket
    /// `allow_experimental_codecs` escape hatch. Unlike `validateCodecString` this classifies instead of
    /// throwing: it is meant for callers that record the session's authorization long before the codec is
    /// resolved (the `temporary_files_codec` spill settings), so an unresolvable codec string is reported
    /// as authorized here and fails later, where it is actually used, with a precise message.
    static bool areCodecGatesSatisfied(const String & compression_codec, const Settings & settings);

    /// Get codec by AST and possible column_type. Some codecs can use
    /// information about type to improve inner settings, but every codec should
    /// be able to work without information about type. Also AST can contain
    /// codec, which can be alias to current default codec, which can be changed
    /// in runtime. If only_generic is true than method will filter all
    /// isGenericCompression() == false codecs from result. If nothing found
    /// will return codec NONE. It's useful for auxiliary parts of complex columns
    /// like Nullable, Array and so on. If all codecs are non generic and
    /// only_generic = true, than codec NONE will be returned.
    CompressionCodecPtr get(const ASTPtr & ast, const IDataType * column_type, CompressionCodecPtr current_default = nullptr, bool only_generic = false) const;

    /// Just wrapper for previous method.
    CompressionCodecPtr get(const ASTPtr & ast, const DataTypePtr & column_type, CompressionCodecPtr current_default = nullptr, bool only_generic = false) const
    {
        return get(ast, column_type.get(), current_default, only_generic);
    }

    /// Get codec by method byte (no params available)
    CompressionCodecPtr get(uint8_t byte_code) const;

    /// For backward compatibility with config settings
    CompressionCodecPtr get(const String & family_name, std::optional<int> level) const;

    /// Get codec by name with optional params. Example: LZ4, ZSTD(3)
    CompressionCodecPtr get(const String & compression_codec) const;

    /// Return a human-readable reason why `compression_codec` (a codec name or chain such as
    /// `"PCO, LZ4"`) can not be safely applied without a column type — because a codec in it
    /// requires a column type or is lossy — or an empty string if it is safe. Experimentality is
    /// not classified here: it is a session-gated policy, not a data-safety property.
    /// Unlike `get(const String &)`, this does NOT throw while resolving a lossy codec (e.g. `SZ3`)
    /// without a column type; it classifies it. This lets callers both reject such a codec on the
    /// create path and normalize (reset) it on the metadata-load path, where throwing would fail the
    /// load. Genuinely invalid codec strings still throw (e.g. `UNKNOWN_CODEC`).
    String getReasonUnsafeForUntypedData(const String & compression_codec) const;

    /// Same as above, but for a codec chain given as a `CODEC(...)` AST (e.g. a stored
    /// `TTL ... RECOMPRESS CODEC(...)` clause), so the caller need not round-trip it through a string.
    String getReasonUnsafeForUntypedData(const ASTPtr & codec_ast) const;

    /// Whether the `CODEC(...)` AST consists of exactly the `Default` alias and nothing else. Callers
    /// that resolve codecs without a column type (e.g. `MergeTreeData::getCompressionCodecForPart`) use
    /// this to treat `CODEC(Default)` as "no forced codec, follow the normal default selection"
    /// (the `default_compression_codec` setting, then the server `<compression>` selector) instead of
    /// resolving the alias to the factory's hardcoded fallback codec.
    static bool isDefaultCodecAlias(const ASTPtr & codec_ast);

    /// Whether the `CODEC(...)` AST contains the `Default` alias anywhere in the chain (e.g.
    /// `CODEC(Delta, Default)`). Callers that resolve such a chain without a column type must supply
    /// a `current_default` obtained from the same normal default selection, so the alias does not
    /// silently resolve to the factory's hardcoded fallback codec.
    static bool containsDefaultCodecAlias(const ASTPtr & codec_ast);
    /// Names of the dedicated settings gating registered codec families.
    Strings getGateSettingNames() const;

    /// Insert codec information into MutableColumns to show in the system table
    void fillCodecDescriptions(MutableColumns & res_columns) const;

    /// The embedded documentation of every registered codec, as a (family name, documentation) pair. The
    /// description is the codec's `getDescription` and the `source` is the file where the codec was registered.
    /// Used by `system.documentation`.
    VectorWithMemoryTracking<std::pair<String, Documentation>> getCodecDocumentations() const;

    /// Register codec with parameters and column type. The `source` is captured automatically at the call site
    /// (the codec's registration), so it points to the source file that defines the codec; do not pass it explicitly.
    void registerCompressionCodecWithType(const String & family_name, std::optional<uint8_t> byte_code, CreatorWithType creator, std::source_location source = std::source_location::current());
    /// Register codec with parameters
    void registerCompressionCodec(const String & family_name, std::optional<uint8_t> byte_code, Creator creator, std::source_location source = std::source_location::current());

    /// Register codec without parameters
    void registerSimpleCompressionCodec(const String & family_name, std::optional<uint8_t> byte_code, SimpleCreator creator, std::source_location source = std::source_location::current());

    Strings getAllRegisteredNames() const;

protected:
    CompressionCodecPtr getImpl(const String & family_name, const ASTPtr & arguments, const IDataType * column_type) const;

private:
    /// Upper-cases the codec family names of a parsed `CODEC(...)` clause in place, and nothing else.
    ///
    /// A codec string coming from a setting (`temporary_files_codec`, `default_compression_codec`, ...) is
    /// not necessarily spelled the way the family is registered in the factory, so the family names are
    /// normalized before the lookup. Only the identifiers and function names may be touched: upper-casing
    /// the whole string before parsing would rewrite literal arguments too, turning e.g. `T64('bit')` into
    /// `T64('BIT')`, which the codec rejects with `Wrong modification for T64` - so a valid stored setting
    /// would fail to resolve, and `getReasonUnsafeForUntypedData` would throw instead of classifying it.
    static void upperCaseCodecFamilyNames(const ASTPtr & codec_ast);

    /// The codec family names of a chain string such as `"PCO, LZ4"`, in order. Throws if the string
    /// cannot be parsed as a codec chain; the classifying callers above catch that.
    static Strings getCodecFamilyNamesOfChain(const String & compression_codec);

    ASTPtr validateCodecAndGetPreprocessedASTImpl(
        const ASTPtr & ast, const DataTypePtr & column_type, const Settings * settings, bool sanity_check) const;

    /// Name of the gate setting: `enable_<lowercase family>_codec`.
    static String getGateSettingName(const String & family_name);

    /// Get setting tier of a codec. nullopt when the codec is ungated or the setting is obsolete (-> codec is GA)
    static std::optional<SettingsTierType> getGateTier(const String & gate_setting_name);

    CompressionCodecsDictionary family_name_with_codec;
    CompressionCodecsCodeDictionary family_code_with_codec;
    /// The source file where each codec family was registered, keyed by family name. See `getCodecDocumentations`.
    UnorderedMapWithMemoryTracking<String, const char *> family_name_with_source;
    CompressionCodecPtr default_codec;

    CompressionCodecFactory();
};

}
