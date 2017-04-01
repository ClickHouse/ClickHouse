#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MarkInCompressedFile.h>


namespace DB
{

class CompressedReadBufferFromFile;


/** Формат Native может содержать отдельно расположенный индекс,
  *  который позволяет понять, где какой столбец расположен,
  *  и пропускать ненужные столбцы.
  */

/** Позиция одного кусочка одного столбца. */
struct IndexOfOneColumnForNativeFormat
{
    String name;
    String type;
    MarkInCompressedFile location;
};

/** Индекс для блока данных. */
struct IndexOfBlockForNativeFormat
{
    using Columns = std::vector<IndexOfOneColumnForNativeFormat>;

    size_t num_columns;
    size_t num_rows;
    Columns columns;
};

/** Весь индекс. */
struct IndexForNativeFormat
{
    using Blocks = std::vector<IndexOfBlockForNativeFormat>;
    Blocks blocks;

    IndexForNativeFormat() {}

    IndexForNativeFormat(ReadBuffer & istr, const NameSet & required_columns)
    {
        read(istr, required_columns);
    }

    /// Прочитать индекс, только для нужных столбцов.
    void read(ReadBuffer & istr, const NameSet & required_columns);
};


/** Десериализует поток блоков из родного бинарного формата (с именами и типами столбцов).
  * Предназначено для взаимодействия между серверами.
  *
  * Также может использоваться для хранения данных на диске.
  * В этом случае, может использовать индекс.
  */
class NativeBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** В случае указания ненулевой server_revision, может ожидаться и считываться дополнительная информация о блоке,
      * в зависимости от поддерживаемой для указанной ревизии.
      *
      * index - не обязательный параметр. Если задан, то будут читаться только указанные в индексе кусочки столбцов.
      */
    NativeBlockInputStream(
        ReadBuffer & istr_, UInt64 server_revision_ = 0,
        bool use_index_ = false,
        IndexForNativeFormat::Blocks::const_iterator index_block_it_ = IndexForNativeFormat::Blocks::const_iterator{},
        IndexForNativeFormat::Blocks::const_iterator index_block_end_ = IndexForNativeFormat::Blocks::const_iterator{});

    String getName() const override { return "Native"; }

    String getID() const override
    {
        std::stringstream res;
        res << this;
        return res.str();
    }

    static void readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows);

protected:
    Block readImpl() override;

private:
    ReadBuffer & istr;
    UInt64 server_revision;

    bool use_index;
    IndexForNativeFormat::Blocks::const_iterator index_block_it;
    IndexForNativeFormat::Blocks::const_iterator index_block_end;
    IndexOfBlockForNativeFormat::Columns::const_iterator index_column_it;

    /// Если задан индекс, то istr должен быть CompressedReadBufferFromFile.
    CompressedReadBufferFromFile * istr_concrete;
};

}
