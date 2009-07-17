#include <iostream>
#include <fstream>
#include <algorithm>
#include <functional>
#include <cmath>

#include <boost/variant/static_visitor.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/get.hpp>
#include <boost/any.hpp>

#include <Poco/Stopwatch.h>
#include <Poco/Timespan.h>
#include <Poco/Exception.h>

#include <DB/Field.h>
#include <DB/ColumnType.h>


class FieldOutVisitor : public boost::static_visitor<>
{
public:
	FieldOutVisitor(std::ostream & ostr_, unsigned indent_ = 0)
		: ostr(ostr_), indent(indent_) {}


	void operator() (const DB::Int & x) const
	{
		for (unsigned i = 0; i < indent; ++i)
			ostr.put('\t');
		ostr << x << "," << std::endl;
	}

	void operator() (const DB::UInt & x) const
	{
		for (unsigned i = 0; i < indent; ++i)
			ostr.put('\t');
		ostr << x << "," << std::endl;
	}

	void operator() (const DB::String & x) const
	{
		for (unsigned i = 0; i < indent; ++i)
			ostr.put('\t');
		ostr << x << "," << std::endl;
	}

	void operator() (const DB::Null & x) const
	{
		for (unsigned i = 0; i < indent; ++i)
			ostr.put('\t');
		ostr << "NULL," << std::endl;
	}

	void operator() (const DB::FieldVector & x) const
	{
		for (unsigned i = 0; i < indent; ++i)
			ostr.put('\t');
		ostr << '{' << std::endl;

		FieldOutVisitor visitor(ostr, indent + 1);
		std::for_each(x.begin(), x.end(), boost::apply_visitor(visitor));

		for (unsigned i = 0; i < indent; ++i)
			ostr.put('\t');
		ostr << '}' << "," << std::endl;
	}

private:
	std::ostream & ostr;
	unsigned indent;
};


class TimesTwoVisitor : public boost::static_visitor<>
{
public:
	template <typename T> void operator() (T & x) const { x *= 2; }

	void operator() (DB::Null & x) const {}
	void operator() (DB::String & x) const { x = ""; }
	void operator() (DB::FieldVector & x) const
	{
		TimesTwoVisitor visitor;
		std::for_each(x.begin(), x.end(), boost::apply_visitor(visitor));
	}
};


class DynamicTimesTwoVisitor
{
public:
	void operator() (boost::any & x) const
	{
		if (x.type() == typeid(DB::UInt))
			boost::any_cast<DB::UInt&>(x) *= 2;
		else if (x.type() == typeid(DB::String))
			boost::any_cast<DB::String&>(x) = "";
		else
			throw Poco::Exception("Unknown type");
	}
};


int main(int argc, char ** argv)
{
	Poco::Stopwatch stopwatch;

	{
		DB::FieldVector arr;
		DB::Field field(arr);

		DB::FieldVector & vec = boost::get<DB::FieldVector>(field);
		vec.reserve(10000000);

		stopwatch.restart();
		for (int i = 0; i < 10000000; ++i)
		{
			if (i % 100 != 0)
				vec.push_back(DB::UInt(10));
			else
				vec.push_back("http://www.boost.org/doc/libs/1_39_0/doc/html/variant/tutorial.html#variant.tutorial.recursive.recursive-variant");
		}
		stopwatch.stop();
		std::cout << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
		
		stopwatch.restart();
		boost::apply_visitor(TimesTwoVisitor(), field);
		stopwatch.stop();
		std::cout << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
		
		//std::ofstream ostr("/dev/null");
		//boost::apply_visitor(FieldOutVisitor(ostr), field);
		sleep(10);
	}

	{
		std::vector<boost::any> vec;
		vec.reserve(10000000);

		stopwatch.restart();
		for (int i = 0; i < 10000000; ++i)
		{
			if (i % 100 != 0)
				vec.push_back(DB::UInt(10));
			else
				vec.push_back(DB::String("http://www.boost.org/doc/libs/1_39_0/doc/html/variant/tutorial.html#variant.tutorial.recursive.recursive-variant"));
		}
		stopwatch.stop();
		std::cout << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
		
		stopwatch.restart();
		DynamicTimesTwoVisitor visitor;
		std::for_each(vec.begin(), vec.end(), visitor);
		stopwatch.stop();
		Poco::Timestamp::TimeDiff elapsed(stopwatch.elapsed());

		std::cout << static_cast<double>(elapsed) / 1000000 << std::endl;

		sleep(10);
	}
	
	return 0;
}
