#pragma once

#include <cassert>
#include <stdexcept>
#include <string>

/// A lightweight non-owning read-only view into a subsequence of a string.
class StringView
{
public:
	using size_type = size_t;

	static constexpr size_type npos = size_type(-1);

public:
	inline StringView() noexcept
		: str(nullptr)
		, len(0)
	{
	}

	constexpr inline StringView(const char* data_, size_t len_) noexcept
		: str(data_)
		, len(len_)
	{
	}

	inline StringView(const std::string& str) noexcept
		: str(str.data())
		, len(str.size())
	{
	}

	inline const char* data() const noexcept
	{
		return str;
	}

	inline bool empty() const noexcept
	{
		return len == 0;
	}

	inline bool null() const noexcept
	{
		assert(len == 0);
		return str == nullptr;
	}

	inline size_type size() const noexcept
	{
		return len;
	}

public:
	/**
	 * Returns a substring [pos, pos + count).
	 * If the requested substring extends past the end of the string,
	 * or if count == npos, the returned substring is [pos, size()).
	 */
	StringView substr(size_type pos, size_type count = npos) const
	{
		if (pos >= len)
			throw std::out_of_range("pos must be less than len");
		if (pos + count >= len || count == npos)
			return StringView(str + pos, len - pos);
		else
			return StringView(str + pos, count);
	}

public:
	inline operator bool () const noexcept
	{
		return !empty();
	}

private:
	const char* str;
	size_t len;
};


/// It creates StringView from literal constant at compile time.
template <size_t size>
constexpr inline StringView MakeStringView(const char (&str)[size])
{
    return StringView(str, size - 1);
}
