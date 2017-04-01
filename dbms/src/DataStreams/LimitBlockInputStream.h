#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Реализует реляционную операцию LIMIT.
  */
class LimitBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** Если always_read_till_end = false (по-умолчанию), то после чтения достаточного количества данных,
      *  возвращает пустой блок, и это приводит к отмене выполнения запроса.
      * Если always_read_till_end = true - читает все данные до конца, но игнорирует их. Это нужно в редких случаях:
      *  когда иначе, из-за отмены запроса, мы бы не получили данные для GROUP BY WITH TOTALS с удалённого сервера.
      */
    LimitBlockInputStream(BlockInputStreamPtr input_, size_t limit_, size_t offset_, bool always_read_till_end_ = false);

    String getName() const override { return "Limit"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Limit(" << children.back()->getID() << ", " << limit << ", " << offset << ")";
        return res.str();
    }

protected:
    Block readImpl() override;

private:
    size_t limit;
    size_t offset;
    size_t pos = 0;
    bool always_read_till_end;
};

}
