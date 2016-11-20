#include <DB/Common/Collator.h>
#include <DB/Columns/ColumnString.h>


namespace DB
{

int ColumnString::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, const Collator & collator) const
{
	const ColumnString & rhs = static_cast<const ColumnString &>(rhs_);

	return collator.compare(
		reinterpret_cast<const char *>(&chars[offsetAt(n)]), sizeAt(n),
		reinterpret_cast<const char *>(&rhs.chars[rhs.offsetAt(m)]), rhs.sizeAt(m));
}


template <bool positive>
struct lessWithCollation
{
	const ColumnString & parent;
	const Collator & collator;

	lessWithCollation(const ColumnString & parent_, const Collator & collator_) : parent(parent_), collator(collator_) {}

	bool operator()(size_t lhs, size_t rhs) const
	{
		int res = collator.compare(
			reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(lhs)]), parent.sizeAt(lhs),
			reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(rhs)]), parent.sizeAt(rhs));

		return positive ? (res < 0) : (res > 0);
	}
};


void ColumnString::getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, Permutation & res) const
{
	size_t s = offsets.size();
	res.resize(s);
	for (size_t i = 0; i < s; ++i)
		res[i] = i;

	if (limit >= s)
		limit = 0;

	if (limit)
	{
		if (reverse)
			std::partial_sort(res.begin(), res.begin() + limit, res.end(), lessWithCollation<false>(*this, collator));
		else
			std::partial_sort(res.begin(), res.begin() + limit, res.end(), lessWithCollation<true>(*this, collator));
	}
	else
	{
		if (reverse)
			std::sort(res.begin(), res.end(), lessWithCollation<false>(*this, collator));
		else
			std::sort(res.begin(), res.end(), lessWithCollation<true>(*this, collator));
	}
}

}
