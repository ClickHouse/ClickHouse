#pragma once

#include <Core/Block.h>
#include <DataStreams/IRowInputStream.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

class ReadBuffer;


/** Поток для чтения данных в формате TSKV.
  * TSKV - очень неэффективный формат данных.
  * Похож на TSV, но каждое поле записано в виде key=value.
  * Поля могут быть перечислены в произвольном порядке (в том числе, в разных строках может быть разный порядок),
  *  и часть полей может отсутствовать.
  * В имени поля может быть заэскейплен знак равенства.
  * Также, в качестве дополнительного элемента может присутствовать бесполезный фрагмент tskv - его нужно игнорировать.
  */
class TSKVRowInputStream : public IRowInputStream
{
public:
    TSKVRowInputStream(ReadBuffer & istr_, const Block & sample_, bool skip_unknown_);

    bool read(Block & block) override;
    bool allowSyncAfterError() const override { return true; };
    void syncAfterError() override;

private:
    ReadBuffer & istr;
    const Block sample;
    /// Пропускать неизвестные поля.
    bool skip_unknown;

    /// Буфер для прочитанного из потока имени поля. Используется, если его потребовалось скопировать.
    String name_buf;

    /// Хэш-таблица соответствия имя поля -> позиция в блоке. NOTE Можно использовать perfect hash map.
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap name_map;
};

}
