#include <Columns/ColumnCompressed.h>
#include <Common/formatReadable.h>
#include "IO/VarInt.h"
#include <Columns/ColumnFSST.h>

#pragma GCC diagnostic ignored "-Wcast-align"
#pragma GCC diagnostic ignored "-Wcast-qual"
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"

#include <fsst.h>

namespace {
    class FsstCompressor final : public DB::ColumnFSST::Compressor {
        struct Header {
            size_t rows_count{0};
            fsst_encoder_t * encoder;
            UInt32 compressed_size{0};
        };

    public:
        FsstCompressor(
            DB::ColumnFSST::Chars& chars_,
            DB::ColumnFSST::Offsets& offsets_
        ): chars(chars_), offsets(offsets_) {
        }

        void prepare(DB::ReadBuffer & istr) override {
            istr.tryIgnore(9); /* ignore ICodec header */
            memcpy(&header, istr.position(), sizeof(header));
            istr.tryIgnore(sizeof(header));
            chars.resize(header.compressed_size);
            istr.readStrict(reinterpret_cast<char*>(chars.data()), header.compressed_size);

            offsets.reserve(header.rows_count);
            size_t offset{0};
            for (size_t i = 0; i < header.rows_count; ++i) {
                UInt64 size;
                DB::readVarUInt(size, istr);
                offset += size;
                offsets.push_back(offset);
            }
            decoder = fsst_decoder(header.encoder);

            std::cerr << "Preparing completed" << header.rows_count << " " << header.compressed_size << " " << offsets.size() << std::endl;
        }

        String encode(const String& str) const override {
            std::cerr << str << std::endl;
            String encoded;
            encoded.resize(2 * str.size());

            size_t rows_count{1};
            size_t len_in[] = {str.size()};
            const unsigned char * str_in[] = {reinterpret_cast<const unsigned char *>(str.data())};

            size_t len_out[rows_count];
            unsigned char * str_out[rows_count];

            if (fsst_compress(
                header.encoder,
                rows_count,
                len_in,
                str_in,
                2000000,
                reinterpret_cast<unsigned char *>(&encoded),
                len_out,
                str_out)
            < rows_count)
            {
                throw std::runtime_error("FSST compression failed");
            }

            encoded.resize(len_out[0]);

            std::cerr << "Encoding" << str << " " << encoded.size() << std::endl;
            return encoded;
        }

    private:
        DB::ColumnFSST::Chars& chars;
        DB::ColumnFSST::Offsets& offsets;

        mutable fsst_decoder_t decoder;
        Header header;
    };
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
}

void ColumnFSST::doPrepare(ReadBuffer & istr) const {
    compressor = std::make_shared<FsstCompressor>(chars, offsets);
    compressor->prepare(istr);
}

}
