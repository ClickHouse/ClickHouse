#include <Interpreters/BPEVocabularyFactory.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

BPEVocabularyFactory & BPEVocabularyFactory::instance()
{
    static BPEVocabularyFactory factory;
    return factory;
}

BPEVocabularyPtr BPEVocabularyFactory::get(const String & name, const Poco::Util::AbstractConfiguration & config)
{
    const String key = "bpe_vocabularies." + name;
    if (!config.has(key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "There is no BPE vocabulary named '{}'. Vocabularies are declared in the `bpe_vocabularies` "
            "section of the server configuration", name);

    if (!config.has(key + ".pretokenizer"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The BPE vocabulary '{}' is declared without a `pretokenizer`", name);
    const BPEPretokenizer pretokenizer = parseBPEPretokenizer(config.getString(key + ".pretokenizer"));

    const bool has_path = config.has(key + ".path");
    const bool has_inline = config.has(key + ".vocabulary");
    if (has_path == has_inline)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The BPE vocabulary '{}' must be declared with either `path` or `vocabulary`, but {}", name,
            has_path ? "has both" : "has neither");

    String path;
    UInt64 modification_time = 0;
    UInt64 size = 0;
    if (has_path)
    {
        path = config.getString(key + ".path");

        std::error_code error;
        const auto status = std::filesystem::status(path, error);
        if (error || !std::filesystem::is_regular_file(status))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The BPE vocabulary '{}' is declared at {}, which is not a readable file", name, path);

        modification_time = static_cast<UInt64>(std::filesystem::last_write_time(path, error).time_since_epoch().count());
        size = static_cast<UInt64>(std::filesystem::file_size(path, error));
        if (error)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot read the BPE vocabulary '{}' from {}: {}", name, path, error.message());
    }

    String contents;
    if (has_inline)
    {
        contents = config.getString(key + ".vocabulary");
        size = contents.size();
        modification_time = sipHash64(contents);
    }

    {
        std::lock_guard lock(mutex);
        const auto it = loaded.find(name);
        if (it != loaded.end() && it->second.path == path && it->second.pretokenizer == pretokenizer
            && it->second.modification_time == modification_time && it->second.size == size)
            return it->second.vocabulary;
    }

    /// Parsing happens outside the lock: it takes a while for a large vocabulary, and a concurrent
    /// query naming another one should not wait for it. Two queries naming the same one at the same
    /// time parse it twice and the loser's copy is dropped, which is cheaper than holding the lock.
    if (has_path)
    {
        ReadBufferFromFile in(path);
        readStringUntilEOF(contents, in);
    }
    auto vocabulary = BPEVocabulary::parse(contents, pretokenizer);

    std::lock_guard lock(mutex);
    loaded[name] = Entry{path, pretokenizer, modification_time, size, vocabulary};
    return vocabulary;
}

}
