#include <Compression/CompressedReadBuffer.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Common/Exception.h>
#include <Common/FST.h>
#include <Common/formatReadable.h>

#include <iostream>
#include <cstdlib>
#include <filesystem>
#include <functional>

#include <boost/program_options.hpp>

#define printAndExit(...) \
    do \
    { \
        fmt::println(__VA_ARGS__); \
        return 1; \
    } while (false)

namespace
{
std::pair<UInt64, UInt64> readNextStateIdAndArcOutput(DB::ReadBuffer & read_buffer)
{
    UInt64 next_state_id = 0;
    UInt64 arc_output = 0;
    readVarUInt(next_state_id, read_buffer);
    if (next_state_id & 0x1) // output is followed
        readVarUInt(arc_output, read_buffer);
    next_state_id >>= 1;
    return {next_state_id, arc_output};
}
}

int mainEntryClickHouseFstDumpTree(int argc, char ** argv)
{
    try
    {
        boost::program_options::options_description desc("Allowed options");
        desc.add_options()
            ("input,i", boost::program_options::value<std::string>(), "FST input path")
            ("output,o", boost::program_options::value<std::string>(), "Dotgraph output path")
            ("help,h", "produce help message")
            ("states,s", "print states information")
            ("dot,d", "print dotgraph")
            ("labels,l", "print output labels")
        ;

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

        if (options.contains("help"))
        {
            fmt::println("Dump tree of FST by the given path.");
            fmt::println("Usage: {} [options]", argv[0]);
            std::cout << desc << std::endl;
            return 1;
        }

        if (!options.contains("input"))
        {
            fmt::println("Missing FST input path.");
            fmt::println("Usage: {} [options]", argv[0]);
            std::cout << desc << std::endl;
        }

        std::string input_path = options.at("input").as<std::string>();
        while (input_path.back() == '/')
            input_path.pop_back();
        if (!std::filesystem::is_directory(input_path))
            printAndExit("Input path '{}' must be a directory", input_path);

        std::optional<std::string> output_path;
        if (options.contains("output"))
        {
            output_path = options.at("output").as<std::string>();
            while (output_path.value().back() == '/')
                output_path.value().pop_back();
            if (!std::filesystem::is_directory(output_path.value()))
                printAndExit("Output path '{}' must be to a directory", output_path.value());
        }

        bool print_states = options.contains("states");
        bool print_dotgraph = options.contains("dot");
        bool print_labels = options.contains("labels");


        fmt::println("Reading FST index files from '{}'", input_path);

        std::unique_ptr<DB::ReadBufferFromFile> segment_id_read_buffer;
        std::unique_ptr<DB::ReadBufferFromFile> segment_metadata_read_buffer;
        std::unique_ptr<DB::ReadBufferFromFile> dictionary_read_buffer;
        std::unique_ptr<DB::ReadBufferFromFile> postings_read_buffer;
        std::unique_ptr<DB::ReadBufferFromFile> bloom_filter_read_buffer;
        for (const auto & dir_entry : std::filesystem::directory_iterator(input_path))
        {
            const auto& path_as_string = dir_entry.path().string();
            if (path_as_string.ends_with(DB::GinIndexStore::GIN_SEGMENT_ID_FILE_TYPE))
            {
                if (segment_id_read_buffer != nullptr)
                    printAndExit("Segment id file are already initialized at '{}', trying to initialized again at '{}'", segment_id_read_buffer->getFileName(), path_as_string);
                segment_id_read_buffer = std::make_unique<DB::ReadBufferFromFile>(dir_entry.path().string());
            }
            if (path_as_string.ends_with(DB::GinIndexStore::GIN_SEGMENT_METADATA_FILE_TYPE))
            {
                if (segment_metadata_read_buffer != nullptr)
                    printAndExit("Segment metadata file are already initialized at '{}', trying to initialized again at '{}'", segment_metadata_read_buffer->getFileName(), path_as_string);
                segment_metadata_read_buffer = std::make_unique<DB::ReadBufferFromFile>(dir_entry.path().string());
            }
            if (path_as_string.ends_with(DB::GinIndexStore::GIN_DICTIONARY_FILE_TYPE))
            {
                if (dictionary_read_buffer != nullptr)
                    printAndExit("Segment dictionary file are already initialized at '{}', trying to initialized again at '{}'", dictionary_read_buffer->getFileName(), path_as_string);
                dictionary_read_buffer = std::make_unique<DB::ReadBufferFromFile>(dir_entry.path().string());
            }
            if (path_as_string.ends_with(DB::GinIndexStore::GIN_POSTINGS_FILE_TYPE))
            {
                if (postings_read_buffer != nullptr)
                    printAndExit("Segment postings file are already initialized at '{}', trying to initialized again at '{}'", postings_read_buffer->getFileName(), path_as_string);
                postings_read_buffer = std::make_unique<DB::ReadBufferFromFile>(dir_entry.path().string());
            }
            if (path_as_string.ends_with(DB::GinIndexStore::GIN_BLOOM_FILTER_FILE_TYPE))
            {
                if (bloom_filter_read_buffer != nullptr)
                    printAndExit("Segment bloom filter file are already initialized at '{}', trying to initialized again at '{}'", bloom_filter_read_buffer->getFileName(), path_as_string);
                bloom_filter_read_buffer = std::make_unique<DB::ReadBufferFromFile>(dir_entry.path().string());
            }
        }
        if (segment_id_read_buffer == nullptr)
            printAndExit("Cannot find segment id file.");
        if (segment_id_read_buffer == nullptr)
            printAndExit("Cannot find segment metadata file.");
        if (dictionary_read_buffer == nullptr)
            printAndExit("Cannot find segment dictionary file.");
        if (postings_read_buffer == nullptr)
            printAndExit("Cannot find segment postings file.");
        if (bloom_filter_read_buffer == nullptr)
            printAndExit("Cannot find segment bloom filter file.");

        DB::GinIndexStore::Format version;
        uint64_t number_of_segments = 0;
        /// Read segment ids
        {
            uint8_t ver;
            readBinary(ver, *segment_id_read_buffer);
            using FormatAsInt = std::underlying_type_t<DB::GinIndexStore::Format>;
            switch (ver)
            {
                case static_cast<FormatAsInt>(DB::GinIndexStore::Format::v1):
                    version = DB::GinIndexStore::Format::v1;
                    break;
                default:
                    printAndExit("Segment id file is corrupted: unsupported version '{}'", ver);
            }

            readVarUInt(number_of_segments, *segment_id_read_buffer);
            /// It contains the next segment id.
            number_of_segments -= 1;

            fmt::println("Segment version = {} and number of segments = {}", version, number_of_segments);
        }

        /// Read segment metadata
        using GinSegmentDictionaries = std::unordered_map<UInt32, DB::GinSegmentDictionaryPtr>;
        GinSegmentDictionaries segment_dictionaries(number_of_segments);
        if (version == DB::GinIndexStore::Format::v1)
        {
            std::vector<DB::GinIndexSegment> segments(number_of_segments);
            segment_metadata_read_buffer->readStrict(reinterpret_cast<char *>(segments.data()), number_of_segments * sizeof(DB::GinIndexSegment));
            for (UInt32 i = 0; i < number_of_segments; ++i)
            {
                auto seg_dict = std::make_shared<DB::GinSegmentDictionary>();
                seg_dict->postings_start_offset = segments[i].postings_start_offset;
                seg_dict->dict_start_offset = segments[i].dict_start_offset;
                seg_dict->bloom_filter_start_offset = segments[i].bloom_filter_start_offset;
                segment_dictionaries[segments[i].segment_id] = seg_dict;
            }
        }

        /// Read segment dictionaries
        {
            for (UInt32 segment_id = 0; segment_id < number_of_segments; ++segment_id)
            {
                auto it = segment_dictionaries.find(segment_id);
                if (it == segment_dictionaries.end())
                {
                    fmt::println("Invalid segment id {}", segment_id);
                    continue;
                }

                const auto & segment_dict = it->second;
                dictionary_read_buffer->seek(segment_dict->dict_start_offset, SEEK_SET);
                if (version == DB::GinIndexStore::Format::v1)
                {
                    fmt::println(
                        "[Segment {}]: term dictionary (FST) offset = {}, bloom filter offset = {}, postings offset = {}",
                        segment_id,
                        segment_dict->dict_start_offset,
                        segment_dict->bloom_filter_start_offset,
                        segment_dict->postings_start_offset);

                    /// Read bloom filter
                    bloom_filter_read_buffer->seek(segment_dict->bloom_filter_start_offset, SEEK_SET);
                    segment_dict->bloom_filter = DB::GinSegmentDictionaryBloomFilter::deserialize(*bloom_filter_read_buffer);

                    fmt::println(
                        "[Segment {}]: bloom filter size = {}",
                        segment_id,
                        formatReadableSizeWithBinarySuffix(bloom_filter_read_buffer->getPosition() - segment_dict->bloom_filter_start_offset));

                    /// Read posting list
                    postings_read_buffer->seek(segment_dict->postings_start_offset, SEEK_SET);
                    UInt8 serialization = 0;
                    readBinary(serialization, *postings_read_buffer);

                    if (serialization == 1)
                    {
                        fmt::println("[Segment {}]: posting list compression = Roaring + ZSTD", segment_id);
                    }
                    else if (serialization == 2)
                    {
                        fmt::println("[Segment {}]: posting list compression = Delta + PFOR", segment_id);
                    }
                    else
                        fmt::println("[Segment {}]: posting list compression = UNKNOWN", segment_id);

                    /// Read FST size header
                    UInt64 fst_size_header = 0;
                    readVarUInt(fst_size_header, *dictionary_read_buffer);

                    /// Get uncompressed FST size
                    size_t uncompressed_fst_size = fst_size_header >> 1;

                    segment_dict->fst = std::make_unique<DB::FST::FiniteStateTransducer>();
                    segment_dict->fst->getData().clear();
                    segment_dict->fst->getData().resize(uncompressed_fst_size);
                    if (fst_size_header & 0x1) /// FST is compressed
                    {
                        /// Read compressed FST size
                        size_t compressed_fst_size = 0;
                        readVarUInt(compressed_fst_size, *dictionary_read_buffer);

                        /// Read compressed FST blob
                        auto buf = std::make_unique<char[]>(compressed_fst_size);
                        dictionary_read_buffer->readStrict(reinterpret_cast<char *>(buf.get()), compressed_fst_size);

                        const auto & codec = DB::GinIndexCompressionFactory::zstdCodec();
                        codec->decompress(
                            buf.get(),
                            static_cast<UInt32>(compressed_fst_size),
                            reinterpret_cast<char *>(segment_dict->fst->getData().data()));
                        fmt::println(
                            "[Segment {}]: FST uncompressed size = {} | compressed size = {}",
                            segment_id,
                            formatReadableSizeWithBinarySuffix(uncompressed_fst_size),
                            formatReadableSizeWithBinarySuffix(compressed_fst_size));
                    }
                    else
                    {
                        dictionary_read_buffer->readStrict(
                            reinterpret_cast<char *>(segment_dict->fst->getData().data()), uncompressed_fst_size);
                        fmt::println(
                            "[Segment {}]: FST uncompressed size = {}",
                            segment_id,
                            formatReadableSizeWithBinarySuffix(uncompressed_fst_size));
                    }
                }
            }
        }

        /// Dump FST states and dotgraph representation for each segment
        {
            using VisitedStatesSet = std::set<std::tuple<UInt64, UInt64, char>>;
            std::function<void(DB::ReadBufferFromMemory &, DB::WriteBuffer &, VisitedStatesSet &, UInt64, UInt64)> dump_state_info
                = [&dump_state_info](
                      DB::ReadBufferFromMemory & read_buffer,
                      DB::WriteBuffer & dotgraph_wb,
                      VisitedStatesSet & visited_states,
                      UInt64 segment_id,
                      UInt64 state_id)
            {
                DB::FST::State curr_state;
                read_buffer.seek(state_id, SEEK_SET);
                curr_state.readFlag(read_buffer);

                if (curr_state.isFinal())
                {
                    dotgraph_wb << fmt::format("state_{0}[label=\"State off: {0}\",shape=doublecircle];", state_id);
                    return;
                }
                else
                {
                    dotgraph_wb << fmt::format("state_{0}[label=\"State off: {0}\"];", state_id);
                }

                UInt8 number_of_labels = 0;
                std::vector<char> labels;
                if (curr_state.getEncodingMethod() == DB::FST::State::EncodingMethod::Sequential)
                {
                    read_buffer.readStrict(reinterpret_cast<char &>(number_of_labels));
                    labels.resize(number_of_labels);
                    std::ignore = read_buffer.read(labels.data(), number_of_labels);
                }
                else if (curr_state.getEncodingMethod() == DB::FST::State::EncodingMethod::Bitmap)
                {
                    DB::FST::LabelsAsBitmap bmp;
                    bmp.deserialize(read_buffer);
                    for (size_t ch = 0; ch < 256; ++ch)
                        if (bmp.hasLabel(static_cast<char>(ch)))
                            labels.emplace_back(static_cast<char>(ch));
                    number_of_labels = labels.size();
                }
                else
                    return;

                std::vector<std::tuple<UInt64, UInt64, char>> arcs;
                for (size_t i = 0; i < number_of_labels; ++i)
                {
                    auto [next_state_id, arc_output] = readNextStateIdAndArcOutput(read_buffer);
                    if (auto [_, inserted] = visited_states.emplace(state_id, next_state_id, labels[i]); inserted)
                        arcs.emplace_back(next_state_id, arc_output, labels[i]);
                }

                /// Dump next states
                for (const auto & [next_state_id, arc_output, label] : arcs)
                {
                    dump_state_info(read_buffer, dotgraph_wb, visited_states, segment_id, next_state_id);
                    dotgraph_wb << fmt::format("state_{} -> state_{}[label=\"{} | {}\"];", state_id, next_state_id, label, arc_output);
                }
            };

            for (const auto & [segment_id, segment_dict] : segment_dictionaries)
            {
                const auto data = segment_dict->fst->getData();

                DB::ReadBufferFromMemory read_buffer(data.data(), data.size());
                read_buffer.seek(data.size() - 1, SEEK_SET);

                UInt8 length = 0;
                read_buffer.readStrict(reinterpret_cast<char &>(length));

                /// FST contains no terms
                if (length == 0)
                {
                    fmt::println("[Segment {}]: FST does not contain any term", segment_id);
                    continue;
                }

                {
                    /// Read number of states and their size.
                    UInt64 number_of_states = 0;

                    read_buffer.seek(0, SEEK_SET);
                    for (UInt64 state_id = 0; read_buffer.getPosition() < static_cast<off_t>(data.size() - 1 - length); number_of_states++)
                    {
                        auto state_start_pos = state_id;

                        DB::FST::State curr_state;
                        read_buffer.seek(state_id, SEEK_SET);
                        curr_state.readFlag(read_buffer);

                        UInt8 number_of_labels = 0;
                        std::vector<char> labels;
                        std::string encoding = "unknown";
                        if (curr_state.getEncodingMethod() == DB::FST::State::EncodingMethod::Sequential)
                        {
                            encoding = "sequential";
                            read_buffer.readStrict(reinterpret_cast<char &>(number_of_labels));
                            labels.resize(number_of_labels);
                            std::ignore = read_buffer.read(labels.data(), number_of_labels);
                        }
                        else if (curr_state.getEncodingMethod() == DB::FST::State::EncodingMethod::Bitmap)
                        {
                            encoding = "bitmap";
                            DB::FST::LabelsAsBitmap bmp;
                            bmp.deserialize(read_buffer);
                            for (size_t i = 0; i < 256; ++i)
                            {
                                char ch = static_cast<char>(i);
                                if (bmp.hasLabel(ch))
                                {
                                    auto arc_index = bmp.getIndex(ch) - 1;
                                    if (arc_index != static_cast<UInt64>(number_of_labels))
                                        fmt::println(
                                            "[Segment {}][State off {}]: Unexpected arc index for '{}'. expected {}, but got {}",
                                            segment_id,
                                            state_id,
                                            std::to_string(ch),
                                            std::to_string(number_of_labels),
                                            arc_index);
                                    labels.emplace_back(ch);
                                    number_of_labels++;
                                }
                            }
                        }

                        for (size_t i = 0; i < number_of_labels; ++i)
                            readNextStateIdAndArcOutput(read_buffer);

                        std::string empty_labels_info;
                        {
                            UInt8 empty_labels = 0;
                            for (const auto & label : labels)
                                empty_labels += label == 0x0;
                            if (empty_labels)
                            {
                                if (empty_labels == number_of_labels)
                                    empty_labels_info = fmt::format("| all labels are dirty");
                                else
                                    empty_labels_info = fmt::format("| empty labels = {}", std::to_string(empty_labels));
                            }
                        }

                        auto state_end_pos = read_buffer.getPosition();

                        DB::WriteBufferFromOwnString output_labels;
                        if (print_labels)
                        {
                            output_labels << "[";
                            for (char label : labels)
                                output_labels << label << ",";
                            output_labels << "] ";
                        }

                        if (print_states)
                            fmt::println(
                                "[Segment {}][State off {}]: size = {} | labels = {} {}| encoding = '{}'{}",
                                segment_id,
                                state_id,
                                formatReadableSizeWithBinarySuffix(state_end_pos - state_start_pos),
                                std::to_string(number_of_labels),
                                output_labels.str(),
                                encoding,
                                empty_labels_info);

                        state_id = state_end_pos;
                    }
                    fmt::println("[Segment {}]: FST number of states = {}", segment_id, number_of_states);
                }

                read_buffer.seek(data.size() - 1 - length, SEEK_SET);
                UInt64 state_index = 0;
                readVarUInt(state_index, read_buffer);

                if (print_states)
                    fmt::println("[Segment {}]: FST start state offset = {}", segment_id, state_index);

                std::string dotgraph;
                DB::WriteBufferFromString dotgraph_wb(dotgraph);
                VisitedStatesSet visited_states;

                dotgraph_wb << "digraph {";
                dump_state_info(read_buffer, dotgraph_wb, visited_states, segment_id, state_index);
                dotgraph_wb << "}";
                dotgraph_wb.finalize();

                if (print_dotgraph)
                    fmt::println("[Segment {}]: FST as dotgraph:\n{}\n", segment_id, dotgraph);

                if (output_path.has_value())
                {
                    std::string file_path = fmt::format("{}/segment_{}.dot", output_path.value(), segment_id);
                    DB::WriteBufferFromFile write_buffer(file_path);
                    DB::writeText(dotgraph, write_buffer);
                    write_buffer.finalize();
                    fmt::println("[Segment {}]: FST as dotgraph saved into {}", segment_id, file_path);
                }
            }
        }

        return 0;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        return 1;
    }
}
