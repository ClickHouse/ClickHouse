#pragma once
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

class OneInputFormat final : public IInputFormat
{
public:
    OneInputFormat(const Block & header, ReadBuffer & in_);

    String getName() const override { return "One"; }

protected:
    Chunk read() override;

private:
    bool done = false;
};

class OneSchemaReader: public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override
    {
        return {{"dummy", std::make_shared<DataTypeUInt8>()}};
    }
};

}
