#include <Parsers/IParserBase.h>

#include <Parsers/formatAST.h>
#include <IO/WriteBufferFromOStream.h>
#include <iostream>

namespace DB
{

bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected)
{
    expected.add(pos, getName());

    return wrapParseImpl(pos, IncreaseDepthTag{}, [&]
    {
//        std::cerr << pos.depth << " 0x" << static_cast<const void*>(this) << " " << getName() << " parsing \"" << pos.get().begin << "\" ... " << std::endl;
        bool res = parseImpl(pos, node, expected);
//        std::cerr << pos.depth << " 0x" << static_cast<const void*>(this) << " " << getName() << " " << (res ? "OK" : "FAIL") << std::endl;
        if (!res)
            node = nullptr;
//        else if (node)
//        {
//            std::cerr << pos.depth << " 0x" << static_cast<const void*>(this) << "\t" << std::ends;
//            {
//            WriteBufferFromOStream out(std::cerr, 4096);
//            formatAST(*node, out);
//            }
//            std::cerr << std::endl;
//        }
        return res;
    });
}

}
