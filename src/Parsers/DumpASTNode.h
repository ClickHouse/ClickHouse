#pragma once

#include <common/logger_useful.h>
#include <Poco/Util/Application.h>
#include <IO/Operators.h>

#include <Parsers/IAST.h>

namespace DB
{

/// If output stream set dumps node with indents and some additional info. Do nothing otherwise.
/// Allow to print key-value pairs inside of tree dump.
class DumpASTNode
{
public:
    DumpASTNode(const IAST & ast_, WriteBuffer * ostr_, size_t & depth, const char * label_ = nullptr)
        : ast(ast_),
        ostr(ostr_),
        indent(depth),
        visit_depth(depth),
        label(label_)
    {
        if (!ostr)
            return;
        if (label && visit_depth == 0)
            (*ostr) << "-- " << label << '\n';
        ++visit_depth;

        (*ostr) << String(indent, ' ');
        printNode();
        (*ostr) << '\n';
    }

    ~DumpASTNode()
    {
        if (!ostr)
            return;
        --visit_depth;
        if (label && visit_depth == 0)
            (*ostr) << "--\n";
    }

    template <typename T, typename U>
    void print(const T & name, const U & value, const char * str_indent = nullptr) const
    {
        if (!ostr)
            return;

        (*ostr) << (str_indent ? String(str_indent) : String(indent, ' '));
        (*ostr) << '(' << name << ' ' << value << ')';
        if (!str_indent)
            (*ostr) << '\n';
    }

    size_t & getDepth() { return visit_depth; }

private:
    const IAST & ast;
    WriteBuffer * ostr;
    size_t indent;
    size_t & visit_depth; /// shared with children
    const char * label;

    String nodeId() const { return ast.getID(' '); }

    void printNode() const
    {
        (*ostr) << nodeId();

        String alias = ast.tryGetAlias();
        if (!alias.empty())
            print("alias", alias, " ");

        if (!ast.children.empty())
            print("children", ast.children.size(), " ");
    }
};

inline void dumpAST(const IAST & ast, WriteBuffer & ostr, DumpASTNode * parent = nullptr)
{
    size_t depth = 0;
    DumpASTNode dump(ast, &ostr, (parent ? parent->getDepth() : depth));

    for (const auto & child : ast.children)
        dumpAST(*child, ostr, &dump);
}


/// String stream dumped in dtor
template <bool _enable>
class DebugASTLog
{
public:
    DebugASTLog()
        : log(nullptr)
    {
        if constexpr (_enable)
            log = &Poco::Logger::get("AST");
    }

    ~DebugASTLog()
    {
        if constexpr (_enable)
            LOG_DEBUG(log, buf.str());
    }

    WriteBuffer * stream() { return (_enable ? &buf : nullptr); }

private:
    Poco::Logger * log;
    WriteBufferFromOwnString buf;
};


}
