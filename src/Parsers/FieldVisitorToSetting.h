#pragma once

#include <Common/FieldVisitorToString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

/// SQL setting-value syntax: maps as `{k:v, ...}` rather than `[...]`, so `SET` and
/// `CREATE CLUSTER` survive a format and re-parse.
class FieldVisitorToSetting : public StaticVisitor<String>
{
public:
    template <class T>
    String operator()(const T & x) const
    {
        FieldVisitorToString visitor;
        return visitor(x);
    }

    String operator()(const Map & x) const
    {
        WriteBufferFromOwnString wb;
        wb << '{';
        for (auto it = x.begin(); it != x.end(); ++it)
        {
            if (it != x.begin())
                wb << ", ";
            wb << applyVisitor(*this, *it);
        }
        wb << '}';
        return wb.str();
    }

    String operator()(const Tuple & x) const
    {
        WriteBufferFromOwnString wb;
        for (auto it = x.begin(); it != x.end(); ++it)
        {
            if (it != x.begin())
                wb << ":";
            wb << applyVisitor(*this, *it);
        }
        return wb.str();
    }
};

}
