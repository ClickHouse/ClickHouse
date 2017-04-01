#pragma once

#include <Common/Exception.h>

namespace DB
{

namespace Conditional
{

enum class CondErrorCodes
{
    TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE,
    TYPE_DEDUCER_UPSCALING_ERROR,
    NUMERIC_PERFORMER_ILLEGAL_COLUMN,
    COND_SOURCE_ILLEGAL_COLUMN,
    NUMERIC_EVALUATOR_ILLEGAL_ARGUMENT,
    ARRAY_EVALUATOR_INVALID_TYPES
};

/// Since the building blocks of the multiIf function may be called
/// in various contexts, their error management must be achieved in
/// a context-free manner. Hence the need for the following class.
class CondException : public DB::Exception
{
public:
    CondException(CondErrorCodes code_, const std::string & msg1_ = "", const std::string & msg2_ = "")
        : code{code_}, msg1{msg1_}, msg2{msg2_}
    {
    }

    const char * name() const throw() override { return "DB::Conditional::Exception"; }
    const char * className() const throw() override { return "DB::Conditional::Exception"; }
    CondException * clone() const override { return new CondException{*this}; }
    void rethrow() const override { throw *this; }
    CondErrorCodes getCode() const { return code; }
    std::string getMsg1() const { return msg1; }
    std::string getMsg2() const { return msg2; }

private:
    CondErrorCodes code;
    std::string msg1;
    std::string msg2;
};

}

}
