#include <Common/FrequencyHolder.h>

#if USE_NLP

#include <incbin.h>

/// Embedded SQL definitions
INCBIN(resource_charset_zst, SOURCE_DIR "/contrib/nlp-data/charset.zst");
INCBIN(resource_tonality_ru_zst, SOURCE_DIR "/contrib/nlp-data/tonality_ru.zst");
INCBIN(resource_programming_zst, SOURCE_DIR "/contrib/nlp-data/programming.zst");


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}


FrequencyHolder & FrequencyHolder::getInstance()
{
    static FrequencyHolder instance;
    return instance;
}

FrequencyHolder::FrequencyHolder()
{
    loadEmotionalDict();
    loadEncodingsFrequency();
    loadProgrammingFrequency();
}

void FrequencyHolder::loadEncodingsFrequency()
{
    LoggerPtr log = getLogger("EncodingsFrequency");

    LOG_TRACE(log, "Loading embedded charset frequencies");

    std::string_view resource(reinterpret_cast<const char *>(gresource_charset_zstData), gresource_charset_zstSize);
    if (resource.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded charset frequencies");

    String line;
    UInt16 bigram;
    Float64 frequency;
    String charset_name;

    auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
    ZstdInflatingReadBuffer in(std::move(buf));

    while (!in.eof())
    {
        readString(line, in);
        in.ignore();

        if (line.empty())
            continue;

        ReadBufferFromString buf_line(line);

        // Start loading a new charset
        if (line.starts_with("// "))
        {
            // Skip "// "
            buf_line.ignore(3);
            readString(charset_name, buf_line);

            /* In our dictionary we have lines with form: <Language>_<Charset>
            * If we need to find language of data, we return <Language>
            * If we need to find charset of data, we return <Charset>.
            */
            size_t sep = charset_name.find('_');

            Encoding enc;
            enc.lang = charset_name.substr(0, sep);
            enc.name = charset_name.substr(sep + 1);
            encodings_freq.push_back(std::move(enc));
        }
        else
        {
            readIntText(bigram, buf_line);
            buf_line.ignore();
            readFloatText(frequency, buf_line);

            encodings_freq.back().map[bigram] = frequency;
        }
    }
    LOG_TRACE(log, "Charset frequencies was added, charsets count: {}", encodings_freq.size());
}

void FrequencyHolder::loadEmotionalDict()
{
    LoggerPtr log = getLogger("EmotionalDict");
    LOG_TRACE(log, "Loading embedded emotional dictionary");

    std::string_view resource(reinterpret_cast<const char *>(gresource_tonality_ru_zstData), gresource_tonality_ru_zstSize);
    if (resource.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded emotional dictionary");

    String line;
    String word;
    Float64 tonality;
    size_t count = 0;

    auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
    ZstdInflatingReadBuffer in(std::move(buf));

    while (!in.eof())
    {
        readString(line, in);
        in.ignore();

        if (line.empty())
            continue;

        ReadBufferFromString buf_line(line);

        readStringUntilWhitespace(word, buf_line);
        buf_line.ignore();
        readFloatText(tonality, buf_line);

        StringRef ref{string_pool.insert(word.data(), word.size()), word.size()};
        emotional_dict[ref] = tonality;
        ++count;
    }
    LOG_TRACE(log, "Emotional dictionary was added. Word count: {}", std::to_string(count));
}

void FrequencyHolder::loadProgrammingFrequency()
{
    LoggerPtr log = getLogger("ProgrammingFrequency");

    LOG_TRACE(log, "Loading embedded programming languages frequencies loading");

    std::string_view resource(reinterpret_cast<const char *>(gresource_programming_zstData), gresource_programming_zstSize);
    if (resource.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded programming languages frequencies");

    String line;
    String bigram;
    Float64 frequency;
    String programming_language;

    auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
    ZstdInflatingReadBuffer in(std::move(buf));

    while (!in.eof())
    {
        readString(line, in);
        in.ignore();

        if (line.empty())
            continue;

        ReadBufferFromString buf_line(line);

        // Start loading a new language
        if (line.starts_with("// "))
        {
            // Skip "// "
            buf_line.ignore(3);
            readString(programming_language, buf_line);

            Language lang;
            lang.name = programming_language;
            programming_freq.push_back(std::move(lang));
        }
        else
        {
            readStringUntilWhitespace(bigram, buf_line);
            buf_line.ignore();
            readFloatText(frequency, buf_line);

            StringRef ref{string_pool.insert(bigram.data(), bigram.size()), bigram.size()};
            programming_freq.back().map[ref] = frequency;
        }
    }
    LOG_TRACE(log, "Programming languages frequencies was added");
}

}

#endif
