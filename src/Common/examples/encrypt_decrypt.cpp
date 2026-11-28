#include <iostream>
#include <Compression/CompressionCodecEncrypted.h>
#include <Compression/ICompressionCodec.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/EventNotifier.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>


/** This program encrypts or decrypts text values using a symmetric encryption codec like AES_128_GCM_SIV or AES_256_GCM_SIV.
  * Keys for codecs are loaded from <encryption_codecs> section of configuration file via value given in config/env/ZooKeeper.
  *
  * How to use:
  *     ./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV text_to_encrypt
  *
  * config.xml example:
<clickhouse>
    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex>530933fba27708642288fecc4ec02c96</key_hex>
            <!--key_hex from_env="CLICK_KEY"/-->
            <!--key_hex from_zk="/clickhouse/key128"/-->
        </aes_128_gcm_siv>
    </encryption_codecs>

    <zookeeper>
        <node>
            <host>localhost</host>
            <port>2181</port>
        </node>
        <node>
            <host>localhost</host>
            <port>2182</port>
        </node>
        <node>
            <host>localhost</host>
            <port>2183</port>
        </node>
    </zookeeper>
</clickhouse>
  */


/// Instance of EncryptDecryptApplication is needed in order to initialize Poco::Net::SSLManager for certificates loading
class EncryptDecryptApplication : public Poco::Util::Application
{
};

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
                      << "codec: AES_128_GCM_SIV or AES_256_GCM_SIV." << std::endl
                      << std::endl
                      << "Example:" << std::endl
                      << "    ./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV text_to_encrypt";
            return 3;
        }

        std::string action = argv[2];
        std::string codec_name = argv[3];
        std::string value = argv[4];

        DB::ConfigProcessor processor(argv[1], false, true);
        bool has_zk_includes;
        DB::XMLDocumentPtr config_xml = processor.processConfig(&has_zk_includes);
        if (has_zk_includes)
        {
            DB::EventNotifier::init(); // Event notifier is needed for correct ZK work

            DB::ConfigurationPtr bootstrap_configuration(new Poco::Util::XMLConfiguration(config_xml));

            zkutil::validateZooKeeperConfig(*bootstrap_configuration);

            EncryptDecryptApplication app;
            Poco::Util::LayeredConfiguration & conf = Poco::Util::Application::instance().config();
            conf.add(bootstrap_configuration);

            zkutil::ZooKeeperArgs args(*bootstrap_configuration, bootstrap_configuration->has("zookeeper") ? "zookeeper" : "keeper");
            auto zookeeper = zkutil::ZooKeeper::createWithoutKillingPreviousSessions(std::move(args));

            zkutil::ZooKeeperNodeCache zk_node_cache([&] { return zookeeper; });
            config_xml = processor.processConfig(&has_zk_includes, &zk_node_cache);
        }
        DB::ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

        DB::CompressionCodecEncrypted::Configuration::instance().load(*configuration, "encryption_codecs");

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
