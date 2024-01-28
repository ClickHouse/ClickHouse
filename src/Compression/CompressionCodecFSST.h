#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include "base/types.h"
#include "fsst12.h"

#include <fsst.h>
#include <vector>

namespace DB
{

class CompressionCodecFSST : public ICompressionCodec
{
public:
    explicit CompressionCodecFSST() {
        setCodecDescription("FSST");
    }

    uint8_t getMethodByte() const override;

    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override {
        FsstParams params = FsstParams(reinterpret_cast<const unsigned char*>(source), source_size);
        fsst_encoder_t *encoder = fsst_create(params.n, params.len_in.data(),
                    const_cast<unsigned char**>(params.str_in.data()), 1);
        fsst_export(encoder, reinterpret_cast<unsigned char*>(dest));

        lenOut.reserve(params.n);
        strOut.reserve(params.n);
        fsst_compress(encoder, params.n, params.len_in.data(),
            const_cast<unsigned char**>(params.str_in.data()),
            source_size + FSST_MAXHEADER, reinterpret_cast<unsigned char*>(dest),
            lenOut.data(), strOut.data());
        fsst_destroy(encoder);

        // if (fsst_compress(encoder, 1, &srcLen[swap], &srcBuf[swap], FSST_MEMBUF*2, dstMem[swap]+FSST_MAXHEADER+3,
        //                                &dstLen[swap], &dstBuf[swap]) < 1) return -1;
    }
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

    void RebuildSymbolTable();

private:
    mutable std::vector<size_t> lenOut;
    mutable std::vector<unsigned char*> strOut;
    struct FsstParams {
        size_t n;
        std::vector <size_t> len_in;
        std::vector <const unsigned char*> str_in;

        explicit FsstParams(const unsigned char* start, UInt32 size) {
            for (UInt32 i = 0; i < size; ++i) {
                if (start[i] == '\0') {
                    ++n;
                }
            }
            len_in.reserve(n);
            str_in.reserve(n);
            str_in.push_back(start);
            for (UInt32 i = 0; i < size; ++i) {
                if (start[i] == '\0') {
                    len_in.push_back(start + i - str_in.back());
                    str_in.push_back(start + i + 1);
                }
            }
            str_in.pop_back();
        }
    }
};

};
