#include <Common/Config/ConfigProcessor.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionCodecEncrypted.h>
#include <iostream>


int main(int argc, char ** argv)
{
    try
    {
        if (argc != 5)
        {
            std::cerr << "usage: " << argv[0] << " path action codec value" << std::endl;
            return 3;
        }

        std::string action = argv[2];
        std::string codec_name = argv[3];
        std::string value = argv[4];
        DB::ConfigProcessor processor(argv[1], false, true);

        auto loaded_config = processor.loadConfig();

        DB::CompressionCodecEncrypted::Configuration::instance().tryLoad(*loaded_config.configuration, "encryption_codecs");

        if (action == "-e")
            std::cout << processor.encryptValue(codec_name, value) << std::endl;
        else if (action == "-d")
            std::cout << processor.decryptValue(codec_name, value) << std::endl;
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
