#include <math.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

void IBlockInputStream::dumpTree(std::ostream & ostr, size_t indent)
{
	ostr << indent + 1 << ". " << getName() << "." << std::endl;

	/// Для красоты
	size_t width = log10(indent + 1) + 4 + getName().size();
	for (size_t i = 0; i < width; ++i)
		ostr << "─";
	ostr << std::endl;

	/// Информация профайлинга, если есть
	if (IProfilingBlockInputStream * profiling = dynamic_cast<IProfilingBlockInputStream *>(this))
	{
		if (profiling->getInfo().blocks != 0)
		{
			profiling->getInfo().print(ostr);
			ostr << std::endl;
		}
	}
	
	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		(*it)->dumpTree(ostr, indent + 1);
}

}

