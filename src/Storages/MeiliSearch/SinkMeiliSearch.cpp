#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONRowOutputFormat.h>
#include <Storages/MeiliSearch/SinkMeiliSearch.h>
#include <base/JSON.h>
#include <base/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEILISEARCH_EXCEPTION;
}

SinkMeiliSearch::SinkMeiliSearch(const MeiliSearchConfiguration & config_, const Block & sample_block_, ContextPtr local_context_)
    : SinkToStorage(sample_block_), connection(config_), local_context{local_context_}, sample_block{sample_block_}
{
}

// gets the content of the json data section, which was obtained using the JSON format output
// "data": [{...}, {...}, {...}]
void extractData(std::string_view & view)
{
    size_t ind = view.find("\"data\":");
    while (view[ind] != '[')
        ++ind;
    view.remove_prefix(ind);
    size_t bal = ind = 1;
    while (bal > 0)
    {
        if (view[ind] == '[')
            ++bal;
        else if (view[ind] == ']')
            --bal;
        ++ind;
    }
    view.remove_suffix(view.size() - ind);
}

void SinkMeiliSearch::writeBlockData(const Block & block) const
{
    FormatSettings settings = getFormatSettings(local_context);
    settings.json.quote_64bit_integers = false;
    WriteBufferFromOwnString buf;
    auto writer = FormatFactory::instance().getOutputFormat("JSON", buf, sample_block, local_context, {}, settings);
    writer->write(block);
    writer->flush();
    writer->finalize();

    std::string_view vbuf(buf.str());
    extractData(vbuf);

    auto response = connection.updateQuery(vbuf);
    auto jres = JSON(response).begin();
    if (jres.getName() == "message")
        throw Exception(ErrorCodes::MEILISEARCH_EXCEPTION, jres.getValue().toString());
}

void SinkMeiliSearch::consume(Chunk chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    writeBlockData(block);
}

}
