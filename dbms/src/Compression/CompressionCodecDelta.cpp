//#include <Compression/CompressionCodecDelta.h>
//#include <IO/CompressedStream.h>
//#include <Compression/CompressionFactory.h>
//
//
//namespace DB
//{
//
//char CompressionCodecDelta::getMethodByte()
//{
//    return static_cast<char>(CompressionMethodByte::LZ4);
//}
//
//void CompressionCodecDelta::getCodecDesc(String & codec_desc)
//{
//    codec_desc = "DELTA";
//}
//
//size_t CompressionCodecDelta::compress(char * source, size_t source_size, char * dest)
//{
//    /// TODO: use SIMD
//    return LZ4_compress_default(source, dest, source_size, LZ4_COMPRESSBOUND(source_size));
//}
//
//size_t CompressionCodecDelta::decompress(char * source, size_t source_size, char * dest, size_t size_decompressed)
//{
//    LZ4::decompress(source, dest, source_size, size_decompressed, lz4_stat);
//    return size_decompressed;
//}
//
//void registerCodecLZ4(CompressionCodecFactory & factory)
//{
//    factory.registerCompressionCodec("DELTA", static_cast<char>(CompressionMethodByte::DELTA), [&](ASTPtr & parameters)
//    {
//        int width = 1;
//
//        if (arguments && !arguments->children.empty())
//        {
//            const auto children = arguments->children;
//            const ASTIdentifier * identifier = static_cast<const ASTIdentifier *>(children[0].get());
//
//            String delta_type = identifier->name;
//            if (delta_type == "Int8" || delta_type == "UInt8")
//                width = 1;
//            else if (delta_type == "Int16" || delta_type == "UInt16")
//                width = 2;
//            else if (delta_type == "Int32" || delta_type == "UInt32")
//                width = 4;
//            else if (delta_type == "Int64" ||  delta_type == "UInt64")
//                width = 8;
//        }
//
//        return std::make_shared<CompressionCodecDelta>(width);
//    });
//}
//
//}
