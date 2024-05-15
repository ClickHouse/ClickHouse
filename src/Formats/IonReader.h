#pragma once

#include "config.h"

#if USE_ION

#include <IO/ReadBuffer.h>

#include <ionc/ion.h>

namespace DB
{

class ReadBuffer;

using IonDecimalPtr = std::shared_ptr<ION_DECIMAL>;
using IonIntPtr = std::shared_ptr<ION_INT>;
using IonTimestampPtr = std::shared_ptr<ION_TIMESTAMP>;

using Symbol = struct
{
    std::string value;
};

struct IonNull
{
    ION_TYPE tpe;
};

const int MAX_DECIMAL_DIGITS = 10000;

class IonReader
{
private:
    hREADER reader;
    ION_TYPE current_type;
    decContext current_decimal_context;

    ReadBuffer & in;

public:
    IonReader(ReadBuffer & in_);
    ~IonReader();

    template <typename T>
    T read() {
        T val;
        auto err = read(val);
        if (err != IERR_OK)
            printf("ERROR reading value: %d\n", err);
        return val;
    }

    IonIntPtr convertDecimalToInt(IonDecimalPtr & val);
    ION_TYPE currentType() const;
    std::optional<std::string> fieldName() const;
    iERR next();
    bool inStruct();
    iERR stepIn();
    iERR stepOut();
    int depth() const;
    bool isNull() const;
    std::vector<std::string> getAnnotations();

private:
    iERR read(std::string & val);
    iERR read(int32_t & val);
    iERR read(int64_t & val);
    iERR read(IonNull & val);
    iERR read(IonDecimalPtr & val);
    iERR read(IonIntPtr & val);
    iERR read(double & val);
    iERR read(bool & val);
    iERR read(Symbol & val);
    iERR read(IonTimestampPtr & val);
};

}
#endif
