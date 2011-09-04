#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

void IBlockInputStream::dumpTree(std::ostream & ostr, size_t indent)
{
	String indent_str(indent, '-');
	ostr << indent_str << getName() << std::endl;

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

