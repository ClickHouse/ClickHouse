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

    /// nullptr on trusted paths (every gated / suspicious codec is accepted).
    /// Otherwise a gated codec must be enabled by its dedicated setting.
    const Settings * settings = nullptr;

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

    /// Return default codec (currently ZSTD(3))
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
