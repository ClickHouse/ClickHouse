#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionCodecEncrypted.h>
#include <IO/BufferWithOwnMemory.h>
#include <Poco/DOM/AutoPtr.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Text.h>
#include <Poco/NumericString.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include "Common/Exception.h"

inline DB::CompressionCodecPtr getCompressionCodecEncrypted(DB::EncryptionMethod Method)
{
    return std::make_shared<DB::CompressionCodecEncrypted>(Method);
}

namespace
{

using namespace Poco;
using namespace Poco::XML;

/*
Fuzzing data consists of:
    first byte:
        1) length of nonce is in data (for correct work with wrong data from user)
        2) length of key is in data (for correct work with wrong data from user)
        3) is 128 turn on
        4) multiple keys for 128
        5) is 256 turn on
        6) multiple keys for 256
        7) nonce is set
        8) current_key is set

    read_key() will cosist of following steps:
        if (2):
            read 4 byte to know size
            if multiple_keys:
                read id
        else:
            size is chosen correctly according to algorithm

        read size_bytes as a key

    next bytes will have information in this order:
        if (3):
            if (4):
                read count
                for i in range(count):
                    read_key()
            else:
                read_key()
            if (7):
                read_nonce (similar to read_key)
            if (8):
                set current_key

        same for AES_256_GCM_SIV with (5) and (6) instead of (3) and (4)

    This class read data and generate xml documentation.
*/
class XMLGenerator
{
public:
    XMLGenerator(const uint8_t * data, size_t& size);

    /// Try to generate config from input data using algorithm, which is described before class declaration
    void generate();

    /// Size of part, which was used on generating config
    size_t keySize() const;

    /// Get config
    const Poco::AutoPtr<Poco::Util::XMLConfiguration>& getResult() const;

    /// If something happened in generator, it will be true
    bool hasError() const;
private:
    /// generate algorithm section with key and nonce
    bool generateAlgorithmKeys(AutoPtr<Poco::XML::Element>& document_root, std::string name,
                               uint8_t mask_for_algo, uint8_t mask_for_multiple_keys);

    /// move on count bytes stream and increase counter
    /// returns false if some errors occuried
    bool next(ssize_t count=1);

    /// Create a key from data
    ssize_t generateKey(std::string name, bool multiple=false);

    const uint8_t * data;

    size_t start_size;
    size_t keys_size;

    AutoPtr<Poco::XML::Document> xml_document;
    AutoPtr<Poco::XML::Element> algo;
    AutoPtr<Poco::Util::XMLConfiguration> conf;

    uint8_t first_byte;

    bool error;
};

XMLGenerator::XMLGenerator(const uint8_t * Data, size_t& Size): data(Data), start_size(Size),
                          conf(new Poco::Util::XMLConfiguration()), error(false) {}

size_t XMLGenerator::keySize() const { return keys_size; }

const Poco::AutoPtr<Poco::Util::XMLConfiguration>& XMLGenerator::getResult() const { return conf; }

bool XMLGenerator::hasError() const { return error; }

bool XMLGenerator::next(ssize_t count)
{
    /// If negative step - something went wrong
    if (count == -1)
    {
        error = true;
        return false;
    }

    /// move data and increase counter
    keys_size += count;

    /// If get after eof
    if (keys_size >= start_size)
    {
        error = true;
        return false;
    }
    data += count;

    return true;
}

/*
<Key>key</key>
or
<key id=..>key</key>
*/
ssize_t XMLGenerator::generateKey(std::string name, bool multiple)
{
    /// set traditional key size for algorithms
    uint64_t size = 0;
    if (name == "aes_128_gcm_siv")
        size = 16;
    if (name == "aes_256_gcm_siv")
        size = 32;

    /// try to read size from data
    if (first_byte & 0x40)
    {
        size = *(reinterpret_cast<const uint64_t*>(data));
        if (!next(8))
            return -1;
    }
    /// if it is not defined, leave
    if (!size)
        return -1;

    AutoPtr<Poco::XML::Element> key_holder;
    if (multiple)
    {
        /// multiple keys have ids.
        uint64_t id = *(reinterpret_cast<const uint64_t*>(data));
        if (!next(8))
            return -1;

        key_holder = xml_document->createElement("key[id=" + std::to_string(id) + "]");

    }
    else
    {
        key_holder = xml_document->createElement("key");
    }
    AutoPtr<Text> key(xml_document->createTextNode(std::string(data, data + size)));
    key_holder->appendChild(key);
    algo->appendChild(key_holder);

    if (!next(size))
        return -1;
    return size;
}

bool XMLGenerator::generateAlgorithmKeys(
    AutoPtr<Poco::XML::Element>& document_root, std::string name, uint8_t mask_for_algo, uint8_t mask_for_multiple_keys)
{
    /// check if algorithm is enabled, then add multiple keys or single key
    if (first_byte & mask_for_algo)
    {
        algo = xml_document->createElement(name);
        document_root->appendChild(algo);

        if (first_byte & mask_for_multiple_keys)
        {
            uint64_t count = *(reinterpret_cast<const uint64_t*>(data));
            if (!next(8))
                return false;

            for (size_t i = 0; i < count; ++i)
            {
                if (!next(generateKey(name)))
                    return false;
            }
        }
        else
        {
            if (!next(generateKey(name)))
                return false;
        }
    }

    /// add nonce
    if (first_byte & 0x02)
    {
        uint64_t nonce_size = 12;
        if (first_byte & 0x80)
        {
            nonce_size = *(reinterpret_cast<const uint64_t*>(data));
            if (!next(8))
                return false;
        }

        AutoPtr<Poco::XML::Element> nonce_holder(xml_document->createElement("nonce"));
        AutoPtr<Text> nonce(xml_document->createTextNode(std::string(data, data + nonce_size)));
        nonce_holder->appendChild(nonce);
        algo->appendChild(nonce_holder);
    }

    /// add current key id
    if (first_byte & 0x01)
    {
        uint64_t current_key = *(reinterpret_cast<const uint64_t*>(data));
        if (!next(8))
            return false;

        AutoPtr<Poco::XML::Element> cur_key_holder(xml_document->createElement("nonce"));
        AutoPtr<Text> cur_key(xml_document->createTextNode(std::to_string(current_key)));
        cur_key_holder->appendChild(cur_key);
        algo->appendChild(cur_key_holder);
    }

    return true;
}

void XMLGenerator::generate()
{
    AutoPtr<Poco::XML::Element> document_root(xml_document->createElement("encryption_codecs"));
    xml_document->appendChild(document_root);

    /// read first byte for parsing
    first_byte = *data;
    if (!next())
        return;

    if (!generateAlgorithmKeys(document_root, "aes_128_gmc_siv", 0x20, 0x10))
        return;
    if (!generateAlgorithmKeys(document_root, "aes_256_gmc_siv", 0x08, 0x04))
        return;

    conf->load(xml_document);
}

}


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        XMLGenerator generator(data, size);

        generator.generate();
        if (generator.hasError())
            return 0;

        auto config = generator.getResult();
        auto codec_128 = getCompressionCodecEncrypted(DB::AES_128_GCM_SIV);
        auto codec_256 = getCompressionCodecEncrypted(DB::AES_256_GCM_SIV);
        DB::CompressionCodecEncrypted::Configuration::instance().tryLoad(*config, "");

        size_t data_size = size - generator.keySize();

        std::string input = std::string(reinterpret_cast<const char*>(data), data_size);
        fmt::print(stderr, "Using input {} of size {}, output size is {}. \n", input, data_size, input.size() - 31);

        DB::Memory<> memory;
        memory.resize(input.size() + codec_128->getAdditionalSizeAtTheEndOfBuffer());
        codec_128->doDecompressData(input.data(), static_cast<UInt32>(input.size()), memory.data(), static_cast<UInt32>(input.size()) - 31);

        memory.resize(input.size() + codec_128->getAdditionalSizeAtTheEndOfBuffer());
        codec_256->doDecompressData(input.data(), static_cast<UInt32>(input.size()), memory.data(), static_cast<UInt32>(input.size()) - 31);
    }
    catch (...)
    {
    }

    return 0;
}
