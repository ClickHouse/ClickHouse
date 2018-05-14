#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include <ext/singleton.h>


namespace DB
{

    class ICompressionCodec;
    using CodecPtr = std::shared_ptr<const ICompressionCodec>;

    class IAST;
    using ASTPtr = std::shared_ptr<IAST>;


/** Creates a codec object by name of compression algorithm family and parameters, also creates codecs pipe.
  */
    class CompressionCodecFactory final : public ext::singleton<CompressionCodecFactory>
    {
    private:
        using Creator = std::function<CodecPtr(const ASTPtr & parameters)>;
        using SimpleCreator = std::function<CodecPtr()>;
        using CodecsDictionary = std::unordered_map<String, Creator>;

    public:
        CodecPtr get(const String & full_name) const;
        CodecPtr get(const String & family_name, const ASTPtr & parameters) const;
        CodecPtr get(const ASTPtr & ast) const;
        CodecPtr get_pipe(const String & full_declaration) const;

        /// Register a codec family by its name.
        void registerCodec(const String & family_name, Creator creator);

        /// Register a simple codec, that have no parameters.
        void registerSimpleCodec(const String & name, SimpleCreator creator);

    private:
        CodecsDictionary codecs;

        CompressionCodecFactory();
        friend class ext::singleton<CompressionCodecFactory>;
    };

}
