#include <cstdint>
#include <iostream>
#include <fstream>
#include <bzlib.h>
#include <Core/Defines.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>

using namespace DB;

static constexpr size_t BUFFER_SIZE = DBMS_DEFAULT_BUFFER_SIZE;
static const String compressed_file = "/data1/liyang/root/2.bz2";
static const String decompressed_file = "/data1/liyang/root/2.txt";
static bz_stream stream;
static char input_buffer[BUFFER_SIZE];
static char output_buffer[BUFFER_SIZE];

int init()
{
    int ret = BZ2_bzDecompressInit(&stream, 0, 0);
    return ret;
}

int processBlockStreaming(
    size_t input_length, const char * input, size_t * input_bytes_read, size_t * output_length, char ** output, bool * stream_end)
{
    stream.next_in = const_cast<char *>(reinterpret_cast<const char *>(input));
    stream.avail_in = static_cast<unsigned int>(input_length);
    stream.next_out = reinterpret_cast<char *>(*output);
    stream.avail_out = BUFFER_SIZE;

    *stream_end = false;
    *input_bytes_read = 0;
    *output_length = 0;
    while (stream.avail_out > 0 && stream.avail_in > 0)
    {
        *stream_end = false;
        int ret = BZ2_bzDecompress(&stream);

        *output_length = BUFFER_SIZE - stream.avail_out;
        *input_bytes_read = input_length - stream.avail_in;

        if (ret == BZ_DATA_ERROR || ret == BZ_DATA_ERROR_MAGIC)
        {
            return ret;
        }
        else if (ret == BZ_STREAM_END)
        {
            std::cout << "meet stream end" << std::endl;
            *stream_end = true;
            ret = BZ2_bzDecompressEnd(&stream);
            if (ret != BZ_OK)
                return ret;
            ret = BZ2_bzDecompressInit(&stream, 0, 0);
                return ret;
        }
        else if (ret != BZ_OK)
        {
            return ret;
        }
    }
    return 0;
}


int main()
{
    bzero(&stream, sizeof(stream));
    if (init() != 0)
        std::cout << "init failed" << std::endl;

    std::ifstream ifs(compressed_file, std::ios::binary);
    std::ofstream ofs(decompressed_file, std::ios::binary);
    size_t output_length = 0;
    char * output = output_buffer;
    bool stream_end = false;
    size_t input_length = 0;
    const char * input = nullptr;
    size_t input_bytes_read = 0;
    size_t i = 0;
    while (!ifs.eof())
    {
        ifs.read(input_buffer, BUFFER_SIZE);
        input_length = ifs.gcount();
        input = input_buffer;
        bool last = (input_length < BUFFER_SIZE);
        // std::cout << i << ":input_length:" << input_length << std::endl;
        // ofs << "====================" << i << "====================" << std::endl;

        while (input_length > 0)
        {
            int ret = processBlockStreaming(input_length, input, &input_bytes_read, &output_length, &output, &stream_end);
            if (ret != 0)
            {
                std::cout << i << ":failed:" << ret << std::endl;
                break;
            }

            std::cout << i << ":input_length:" << input_length << ":input_bytes_read:" << input_bytes_read
                      << ":output_length:" << output_length << std::endl;
            // ofs << "===" << i << ":input_length:" << input_length << ":input_bytes_read:" << input_bytes_read
                // << ":output_length:" << output_length << "===" << std::endl;
            ofs.write(output_buffer, output_length);

            input_length -= input_bytes_read;
            input += input_bytes_read;
            output = output_buffer;
        }

        if (last)
            break;
        ++i;
    }

    BZ2_bzDecompressEnd(&stream);
    ifs.close();
    ofs.close();
    return 0;
}
