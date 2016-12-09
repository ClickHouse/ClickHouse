#pragma once

#include <cassert>
#include <stdexcept>
#include <string>

/// A lightweight non-owning read-only view into a subsequence of a string.
template <
	typename TChar,
	typename TTraits = std::char_traits<TChar>
>
class StringViewImpl
{
public:
	using size_type = size_t;
	using traits_type = TTraits;
	using value_type = typename TTraits::char_type;

	static constexpr size_type npos = size_type(-1);

public:
	inline StringViewImpl() noexcept
		: str(nullptr)
		, len(0)
	{
	}

	constexpr inline StringViewImpl(const TChar* data_, size_t len_) noexcept
		: str(data_)
		, len(len_)
	{
	}

	inline StringViewImpl(const std::basic_string<TChar>& str) noexcept
		: str(str.data())
		, len(str.size())
	{
	}

	inline TChar at(size_type pos) const
	{
		if (pos >= len)
			throw std::out_of_range("pos must be less than len");
		return str[pos];
	}

	inline TChar back() const noexcept
	{
		return str[len - 1];
	}

	inline const TChar* data() const noexcept
	{
		return str;
	}

	inline bool empty() const noexcept
	{
		return len == 0;
	}

	inline TChar front() const noexcept
	{
		return str[0];
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
	StringViewImpl substr(size_type pos, size_type count = npos) const
	{
		if (pos >= len)
			throw std::out_of_range("pos must be less than len");
		if (pos + count >= len || count == npos)
			return StringViewImpl(str + pos, len - pos);
		else
			return StringViewImpl(str + pos, count);
	}

public:
	inline operator bool () const noexcept
	{
		return !empty();
	}

	inline TChar operator [] (size_type pos) const noexcept
	{
		return str[pos];
	}

	inline bool operator < (const StringViewImpl& other) const noexcept
	{
		if (len < other.len)
			return true;
		if (len > other.len)
			return false;
		return TTraits::compare(str, other.str, len) < 0;
	}

	inline bool operator == (const StringViewImpl& other) const noexcept
	{
		if (len == other.len)
			return TTraits::compare(str, other.str, len) == 0;
		return false;
	}

private:
	const TChar* str;
	size_t len;
};


/// It creates StringView from literal constant at compile time.
template <typename TChar, size_t size>
constexpr inline StringViewImpl<TChar> MakeStringView(const TChar (&str)[size])
{
    return StringViewImpl<TChar>(str, size - 1);
}


using StringView = StringViewImpl<char>;
