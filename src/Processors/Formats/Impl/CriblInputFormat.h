#pragma once

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include "base/types.h"


namespace DB
{


class CriblInputFormat : public IRowInputFormat
{
public:
    CriblInputFormat(const Block & header_, ReadBuffer & in_, const FormatSettings & format_settings, const ReadSettings & read_settings);
    virtual ~CriblInputFormat() override { }

    virtual String getName() const override;
    virtual void resetParser() override;

    /// Not implemented - this format uses generate() instead
    virtual bool readRow(MutableColumns & columns, RowReadExtension & extra) override;


    void readLineObject(MutableColumns & columns);

    size_t countRows(size_t max_block_size) override;
    bool supportsCountRows() const override { return true; }
};

class CriblSchemaReader : public ISchemaReader
{
public:
    /**
     * @brief Construct a new ParquetFlexSchemaReader
     * 
     * @param buf - Input buffer containing the parquet data
     * @param set - Format settings
     */
    CriblSchemaReader(ReadBuffer & buf, const FormatSettings & set);

    virtual ~CriblSchemaReader() override { }

    /// Read and return the schema of the parquet file
    NamesAndTypesList readSchema() override;

private:
    FormatSettings settings;
};


/**
 * @brief Register the ParquetFlex input format with the format factory
 * 
 * @param factory - Format factory to register with
 */
void registerInputFormatCribl(FormatFactory & factory);

void registerFileSegmentationEngineCriblLineAsString(FormatFactory & factory);

}
