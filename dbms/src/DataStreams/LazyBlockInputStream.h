#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Инициализировать другой источник при первом вызове read, и затем использовать его.
  * Это нужно, например, для чтения из таблицы, которая будет заполнена
  *  после создания объекта LazyBlockInputStream, но до первого вызова read.
  */
class LazyBlockInputStream : public IProfilingBlockInputStream
{
public:
    using Generator = std::function<BlockInputStreamPtr()>;

    LazyBlockInputStream(Generator generator_)
        : generator(generator_) {}

    String getName() const override { return "Lazy"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Lazy(" << this << ")";
        return res.str();
    }

protected:
    Block readImpl() override
    {
        if (!input)
        {
            input = generator();

            if (!input)
                return Block();

            children.push_back(input);

            if (IProfilingBlockInputStream * p_input = dynamic_cast<IProfilingBlockInputStream *>(input.get()))
            {
                /// Они могли быть установлены раньше, но не были протащены в input.
                if (progress_callback)
                    p_input->setProgressCallback(progress_callback);
                if (process_list_elem)
                    p_input->setProcessListElement(process_list_elem);
            }
        }

        return input->read();
    }

private:
    Generator generator;
    BlockInputStreamPtr input;
};

}
