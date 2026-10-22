#pragma once

#include <Common/logger_useful.h>
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

class DumpASTNodeInDotFormat
{
public:
    DumpASTNodeInDotFormat(const IAST & ast_, WriteBuffer * ostr_, bool root_ = true, const char * label_ = nullptr)
        : ast(ast_), ostr(ostr_), root(root_), label(label_)
    {
        if (!ostr)
            return;

        if (root)
            (*ostr) << "digraph " << (label ? String(label) : "") << "{\n    rankdir=\"UD\";\n";

        printNode();
    }

    ~DumpASTNodeInDotFormat()
    {
        if (!ostr)
            return;

        for (const auto & child : ast.children)
            printEdge(ast, *child);

        if (root)
            (*ostr) << "}\n";
    }

private:
    const IAST & ast;
    WriteBuffer * ostr;
    bool root;
    const char * label;

    String getASTId() const { return ast.getID(' '); }
    static String getNodeId(const IAST & a) { return "n" + std::to_string(reinterpret_cast<std::uintptr_t>(&a)); }

    void printNode() const
    {
        (*ostr) << "    " << getNodeId(ast) << "[label=\"";
        (*ostr) << getASTId();

        String alias = ast.tryGetAlias();
        if (!alias.empty())
            (*ostr) << " ("
                    << "alias"
                    << " " << alias << ")";

        if (!ast.children.empty())
            (*ostr) << " (children"
                    << " " << ast.children.size() << ")";
        (*ostr) << "\"];\n";
    }

    void printEdge(const IAST & parent, const IAST & child) const
    {
        (*ostr) << "    " << getNodeId(parent) << " -> " << getNodeId(child) << ";\n";
    }
};


/// Print AST in "dot" format for GraphViz
/// You can render it with: dot -Tpng ast.dot ast.png
inline void dumpASTInDotFormat(const IAST & ast, WriteBuffer & ostr, bool root = true)
{
    DumpASTNodeInDotFormat dump(ast, &ostr, root);
    for (const auto & child : ast.children)
        dumpASTInDotFormat(*child, ostr, false);
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
            log = getLogger("AST");
    }

    ~DebugASTLog()
    {
        if constexpr (_enable)
            LOG_DEBUG(log, fmt::runtime(buf.str()));
    }

    WriteBuffer * stream() { return (_enable ? &buf : nullptr); }

private:
    LoggerPtr log;
    WriteBufferFromOwnString buf;
};


}
