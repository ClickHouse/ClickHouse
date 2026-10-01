#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
namespace DB
{
class ArrayConcat : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_concat()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArrayIif : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_iif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArrayIndexOf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_index_of()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArrayLength : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_length()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArrayReverse : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_reverse()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArrayRotateLeft : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_rotate_left()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArrayRotateRight : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_rotate_right()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArrayShiftLeft : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_shift_left()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArrayShiftRight : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_shift_right()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArraySlice : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_slice()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArraySortAsc : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_sort_asc()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArraySortDesc : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_sort_desc()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArraySplit : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_split()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArraySum : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "array_sum()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BagKeys : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bag_keys()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BagMerge : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bag_merge()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BagRemoveKeys : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bag_remove_keys()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class JaccardIndex : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "jaccard_index()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Pack : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "pack()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class PackAll : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "pack_all()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class PackArray : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "pack_array()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Repeat : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "repeat()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SetDifference : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "set_difference()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SetHasElement : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "set_has_element()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SetIntersect : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "set_intersect()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SetUnion : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "set_union()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class TreePath : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "treepath()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Zip : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "zip()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

}
