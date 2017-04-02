#pragma once

#include <Interpreters/Aggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Агрегирует поток блоков, используя заданные столбцы-ключи и агрегатные функции.
  * Столбцы с агрегатными функциями добавляет в конец блока.
  * Если final=false, агрегатные функции не финализируются, то есть, не заменяются на своё значение, а содержат промежуточное состояние вычислений.
  * Это необходимо, чтобы можно было продолжить агрегацию (например, объединяя потоки частично агрегированных данных).
  */
class AggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** keys берутся из GROUP BY части запроса
      * Агрегатные функции ищутся везде в выражении.
      * Столбцы, соответствующие keys и аргументам агрегатных функций, уже должны быть вычислены.
      */
    AggregatingBlockInputStream(BlockInputStreamPtr input_, const Aggregator::Params & params_, bool final_)
        : params(params_), aggregator(params), final(final_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "Aggregating"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Aggregating(" << children.back()->getID() << ", " << aggregator.getID() << ")";
        return res.str();
    }

protected:
    Block readImpl() override;

    Aggregator::Params params;
    Aggregator aggregator;
    bool final;

    bool executed = false;

    /// Для чтения сброшенных во временный файл данных.
    struct TemporaryFileStream
    {
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;

        TemporaryFileStream(const std::string & path);
    };
    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

    /** Отсюда будем доставать готовые блоки после агрегации. */
    std::unique_ptr<IBlockInputStream> impl;

    Logger * log = &Logger::get("AggregatingBlockInputStream");
};

}
