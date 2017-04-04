#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Объединяет несколько источников в один.
  * В отличие от UnionBlockInputStream, делает это последовательно.
  * Блоки разных источников не перемежаются друг с другом.
  */
class ConcatBlockInputStream : public IProfilingBlockInputStream
{
public:
    ConcatBlockInputStream(BlockInputStreams inputs_)
    {
        children.insert(children.end(), inputs_.begin(), inputs_.end());
        current_stream = children.begin();
    }

    String getName() const override { return "Concat"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Concat(";

        Strings children_ids(children.size());
        for (size_t i = 0; i < children.size(); ++i)
            children_ids[i] = children[i]->getID();

        /// Будем считать, что порядок конкатенации блоков не имеет значения.
        std::sort(children_ids.begin(), children_ids.end());

        for (size_t i = 0; i < children_ids.size(); ++i)
            res << (i == 0 ? "" : ", ") << children_ids[i];

        res << ")";
        return res.str();
    }

protected:
    Block readImpl() override
    {
        Block res;

        while (current_stream != children.end())
        {
            res = (*current_stream)->read();

            if (res)
                break;
            else
            {
                (*current_stream)->readSuffix();
                ++current_stream;
            }
        }

        return res;
    }

private:
    BlockInputStreams::iterator current_stream;
};

}
