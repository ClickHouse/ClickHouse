#pragma once

#include <Columns/ColumnString.h>

namespace DB
{

namespace extractKV
{

class StringWriter
{
    ColumnString & col;
    ColumnString::Chars & chars;
    UInt64 prev_commit_pos = 0;

public:
    explicit StringWriter(ColumnString & col_)
        : col(col_),
        chars(col.getChars())
    {}

    ~StringWriter()
    {
        // Make sure that ColumnString invariants are not broken.
        if (!isEmpty())
            reset();
    }

    void append(std::string_view new_data)
    {
        chars.insert(new_data.begin(), new_data.end());
    }

    template <typename T>
    void append(const T * begin, const T * end)
    {
        chars.insert(begin, end);
    }

    void reset()
    {
        chars.resize_assume_reserved(prev_commit_pos);
    }

    bool isEmpty() const
    {
        return chars.size() == prev_commit_pos;
    }

    void commit()
    {
        chars.push_back('\0');
        col.getOffsets().emplace_back(chars.size());
        prev_commit_pos = chars.size();
    }

    std::string_view uncommittedChunk() const
    {
        return std::string_view(chars.raw_data() + prev_commit_pos, chars.raw_data() + chars.size());
    }
};
}

}
