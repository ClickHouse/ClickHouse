#include <Common/Config/ConfigProcessor.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionCodecEncrypted.h>
#include <iostream>

/** This program encrypts or decrypts text values using a symmetric encryption codec like AES_128_GCM_SIV or AES_256_GCM_SIV.
  * Keys for codecs are loaded from <encryption_codecs> section of configuration file.
  *
  * How to use:
  *     ./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV text_to_encrypt
  */

int main(int argc, char ** argv)
{
    try
    {
        if (argc != 5)
        {
            std::cerr << "Usage:" << std::endl
                << "    " << argv[0] << " path action codec value" << std::endl
                << "path: path to configuration file." << std::endl
                << "action: -e for encryption and -d for decryption." << std::endl
                << "codec: AES_128_GCM_SIV or AES_256_GCM_SIV." << std::endl << std::endl
                << "Example:"  << std::endl
                << "    ./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV text_to_encrypt";
            return 3;
        }

        std::string action = argv[2];
        std::string codec_name = argv[3];
        std::string value = argv[4];

        DB::ConfigProcessor processor(argv[1], false, true);
        auto loaded_config = processor.loadConfig();
        DB::CompressionCodecEncrypted::Configuration::instance().load(*loaded_config.configuration, "encryption_codecs");

        if (action == "-e")
            std::cout << DB::ConfigProcessor::encryptValue(codec_name, value) << std::endl;
        else if (action == "-d")
            std::cout << DB::ConfigProcessor::decryptValue(codec_name, value) << std::endl;
        else
            std::cerr << "Unknown action: " << action << std::endl;
    }
    catch (Poco::Exception & e)
    {
        std::cerr << "Exception: " << e.displayText() << std::endl;
        return 1;
    }
    catch (std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        return 3;
    }
    catch (...)
    {
        std::cerr << "Some exception" << std::endl;
        return 2;
    }

    return 0;
}
