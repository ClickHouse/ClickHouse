#pragma once

#include <cassert>
#include <string>

/// A lightweight non-owning read-only view into a subsequence of a string.
class StringView
{
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

	inline size_t size() const noexcept
	{
		return len;
	}

public:
	inline operator bool () const noexcept
	{
		return !empty();
	}

private:
	const char* str;
	const size_t len;
};


/// It creates StringView from literal constant at compile time.
template <size_t size>
constexpr inline StringView MakeStringView(const char (&str)[size])
{
    return StringView(str, size - 1);
}
