#include <math.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{


void IBlockInputStream::dumpTree(std::ostream & ostr, size_t indent)
{
	ostr << String(indent, ' ') << getShortName() << std::endl;

	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		(*it)->dumpTree(ostr, indent + 1);
}


void IBlockInputStream::dumpTreeWithProfile(std::ostream & ostr, size_t indent)
{
	ostr << indent + 1 << ". " << getShortName() << "." << std::endl;

	/// Для красоты
	size_t width = log10(indent + 1) + 4 + getShortName().size();
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
		(*it)->dumpTreeWithProfile(ostr, indent + 1);
}


String IBlockInputStream::getShortName() const
{
	String res = getName();
	if (0 == strcmp(res.c_str() + res.size() - strlen("BlockInputStream"), "BlockInputStream"))
		res = res.substr(0, res.size() - strlen("BlockInputStream"));
	return res;
}


BlockInputStreams IBlockInputStream::getLeaves()
{
	BlockInputStreams res;
	getLeavesImpl(res);
	return res;
}


void IBlockInputStream::getLeavesImpl(BlockInputStreams & res, BlockInputStreamPtr this_shared_ptr)
{
	if (children.empty())
	{
		if (this_shared_ptr)
			res.push_back(this_shared_ptr);
	}
	else
		for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
			(*it)->getLeavesImpl(res, *it);
}


}

