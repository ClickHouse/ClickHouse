#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>

#include <iostream>

int main()
{
	const char input[] = "1 1.0 10.5 115e2 -5 -5.0 10- 7+8 90-3 .5 127.0.0.1 +1 +1-1";
	DB::ReadBuffer buf(const_cast<char *>(input), strlen(input), 0);

	Int64 i;
	double f;
	double Epsilon = 1e-10;
	int t = 0;

#define CHECK(x, y) do { DB::readText(x, buf); ++t; if (((x-y) > Epsilon) || (y-x) > Epsilon) return t; buf.ignore();} while (0);
	CHECK(i, 1);
	CHECK(f, 1.0f);
	CHECK(f, 10.5f);
	CHECK(f, 115e2);
	CHECK(i, -5);
	CHECK(f, -5);
	CHECK(i, 10);
	buf.ignore();
	CHECK(i, 7);
	buf.ignore(2);
	CHECK(i, 90);
	buf.ignore(2);
	/// Интересный случай: хотим ли мы, чтобы .5 парсилось как 0.5? Вроде бы это уместно.
	CHECK(f, 0.5f);
	CHECK(f, 127);
	buf.ignore(4); // "0.1 "
	CHECK(i, 1);
	CHECK(i, 1);
#undef CHECK

	return 0;
}
