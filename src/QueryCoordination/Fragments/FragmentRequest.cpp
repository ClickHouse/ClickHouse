#include <QueryCoordination/Fragments/FragmentRequest.h>

namespace DB
{

void FragmentRequest::write(WriteBuffer & out) const
{
    writeVarUInt(fragment_id, out);

    size_t to_size = data_to.size();
    writeVarUInt(to_size, out);

    for (const String & to : data_to)
        writeStringBinary(to, out);

    size_t from_size = data_from.size();
    writeVarUInt(from_size, out);

    for (const auto & [exchange_id, sources] : data_from)
    {
        writeVarUInt(exchange_id, out);

        size_t e_source_size = sources.size();
        writeVarUInt(e_source_size, out);

        for (const auto & source : sources)
            writeStringBinary(source, out);
    }
}

void FragmentRequest::read(ReadBuffer & in)
{
    readVarUInt(fragment_id, in);

    size_t to_size = 0;
    readVarUInt(to_size, in);
    data_to.reserve(to_size);

    for (size_t i = 0; i < to_size; ++i)
    {
        String to;
        readStringBinary(to, in);
        data_to.emplace_back(to);
    }

    size_t from_size = 0;
    readVarUInt(from_size, in);

    for (size_t i = 0; i < from_size; ++i)
    {
        UInt32 exchange_id;
        readVarUInt(exchange_id, in);
        auto & e_sources = data_from[exchange_id];

        size_t e_source_size;
        readVarUInt(e_source_size, in);
        e_sources.reserve(e_source_size);

        for (size_t j = 0; j < e_source_size; ++j)
        {
            String source;
            readStringBinary(source, in);
            e_sources.emplace_back(source);
        }
    }
}

String FragmentRequest::toString() const
{
    String fragment = "Fragment id " + std::to_string(fragment_id) + ".";
    String data_to_str = " Data to: ";
    for (const auto & to : data_to)
        data_to_str += (to + ", ");

    String data_from_str = "Data from: ";

    for (const auto & [exchange_id, sources] : data_from)
    {
        data_from_str += ("exchange_id " + std::to_string(exchange_id)) + " from: ";

        for (const auto & source : sources)
            data_from_str += (source + ", ");
    }

    return fragment + data_to_str + data_from_str;
}

void FragmentsRequest::write(WriteBuffer & out) const
{
    /// query has been sent

    size_t size = fragments_request.size();
    writeVarUInt(size, out);

    for (const FragmentRequest & fragment_request : fragments_request)
        fragment_request.write(out);
}

void FragmentsRequest::read(ReadBuffer & in)
{
    /// query has been read

    size_t fragment_size = 0;
    readVarUInt(fragment_size, in);
    //        fragments_request.reserve(fragment_size);

    for (size_t i = 0; i < fragment_size; ++i)
    {
        FragmentRequest request;
        request.read(in);
        fragments_request.emplace_back(request);
    }
}

}
