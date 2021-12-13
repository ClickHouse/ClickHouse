#pragma once

#include <CharStream.h>


namespace DB
{

class CharInputStream : public antlr4::CharStream
{
    public:
        CharInputStream(const char * begin, const char * end);

    private:
        const char * d;
        size_t s = 0;
        size_t p = 0;

        size_t index() override { return p; }
        size_t size() override { return s; }

        size_t LA(ssize_t i) override;
        void consume() override;
        void seek(size_t i) override;

        ssize_t mark() override { return -1; }
        void release(ssize_t marker) override {};

        std::string getSourceName() const override { return "CharInputStream"; };
        std::string getText(const antlr4::misc::Interval &interval) override;
        std::string toString() const override { return {d, s}; }
};

}
