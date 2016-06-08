#pragma once

#include <cstddef>

#include <common/likely.h>


namespace DB
{

/** Массив (почти) неизменяемого размера:
  *  размер задаётся в конструкторе;
  *  метод resize приводит к удалению старых данных и нужен лишь для того,
  *   чтобы можно было сначала создать пустой объект, используя конструктор по-умолчанию,
  *   а потом уже определиться с размером.
  *
  * Есть возможность не инициализировать элементы по-умолчанию, а создавать их inplace.
  * Деструкторы элементов вызываются автоматически.
  *
  * sizeof равен размеру одного указателя.
  *
  * Не exception-safe.
  * Копирование не поддерживается. Перемещение опустошает исходный объект.
  * То есть, использовать этот массив во многих случаях неудобно.
  *
  * Предназначен для ситуаций, в которых создаётся много массивов одинакового небольшого размера,
  *  но при этом размер не известен во время компиляции.
  * Также даёт существенное преимущество в случаях, когда важно, чтобы sizeof был минимальным.
  * Например, если массивы кладутся в open-addressing хэш-таблицу с inplace хранением значений (как HashMap)
  *
  * В этом случае, по сравнению с std::vector:
  * - для массивов размером в 1 элемент - преимущество примерно в 2 раза;
  * - для массивов размером в 5 элементов - преимущество примерно в 1.5 раза
  *   (в качестве T использовались DB::Field, содержащие UInt64 и String);
  */

const size_t empty_auto_array_helper = 0;

struct DontInitElemsTag {};

template <typename T>
class AutoArray
{
public:
	/// Для отложенного создания.
	AutoArray()
	{
		setEmpty();
	}
	
	AutoArray(size_t size_)
	{
		init(size_, false);
	}
	
	/** Не будут вызваны конструкторы по-умолчанию для элементов.
	  * В этом случае, вы должны вставить все элементы с помощью функции place и placement new,
	  *  так как для них потом будут вызваны деструкторы.
	  */
	AutoArray(size_t size_, const DontInitElemsTag & tag)
	{
		init(size_, true);
	}
	
	/** Инициализирует все элементы копирующим конструктором с параметром value.
	  */
	AutoArray(size_t size_, const T & value)
	{
		init(size_, true);
		
		for (size_t i = 0; i < size_; ++i)
		{
			new (place(i)) T(value);
		}
	}

	/** resize удаляет все существующие элементы.
	  */
	void resize(size_t size_, bool dont_init_elems = false)
	{
		uninit();
		init(size_, dont_init_elems);
	}

	/** Премещение.
	  */
    AutoArray(AutoArray && src)
	{
		if (this == &src)
			return;
		setEmpty();
		data = src.data;
		src.setEmpty();
	}

	AutoArray & operator= (AutoArray && src)
	{
		if (this == &src)
			return *this;
		uninit();
		data = src.data;
		src.setEmpty();

		return *this;
	}

	~AutoArray()
	{
		uninit();
	}

	size_t size() const
	{
		return m_size();
	}

	bool empty() const
	{
		return size() == 0;
	}

	void clear()
	{
		uninit();
		setEmpty();
	}

	/** Можно читать и модифицировать элементы с помощью оператора []
	  *  только если элементы были инициализированы
	  *  (то есть, в конструктор не был передан DontInitElemsTag,
	  *   или вы их инициализировали с помощью place и placement new).
	  */
	T & operator[](size_t i)
	{
		return elem(i);
	}
	
	const T & operator[](size_t i) const
	{
		return elem(i);
	}

	/** Получить кусок памяти, в котором должен быть расположен элемент.
	  * Функция предназначена, чтобы инициализировать элемент,
	  *  который ещё не был инициализирован:
	  * new (arr.place(i)) T(args);
	  */
	char * place(size_t i)
	{
		return data + sizeof(T) * i;
	}

	using iterator = T *;
	using const_iterator = const T *;

	iterator begin() { return &elem(0); }
	iterator end() { return &elem(size()); }

	const_iterator begin() const { return &elem(0); }
	const_iterator end() const { return &elem(size()); }

	bool operator== (const AutoArray<T> & rhs) const
	{
		size_t s = size();

		if (s != rhs.size())
			return false;

		for (size_t i = 0; i < s; ++i)
			if (elem(i) != rhs.elem(i))
				return false;

		return true;
	}

	bool operator!= (const AutoArray<T> & rhs) const
	{
		return !(*this == rhs);
	}

	bool operator< (const AutoArray<T> & rhs) const
	{
		size_t s = size();
		size_t rhs_s = rhs.size();

		if (s < rhs_s)
			return true;
		if (s > rhs_s)
			return false;

		for (size_t i = 0; i < s; ++i)
		{
			if (elem(i) < rhs.elem(i))
				return true;
			if (elem(i) > rhs.elem(i))
				return false;
		}

		return false;
	}

private:
	char * data;

	size_t & m_size()
	{
		return reinterpret_cast<size_t *>(data)[-1];
	}

	size_t m_size() const
	{
		return reinterpret_cast<const size_t *>(data)[-1];
	}

	T & elem(size_t i)
	{
		return reinterpret_cast<T *>(data)[i];
	}

	const T & elem(size_t i) const
	{
		return reinterpret_cast<const T *>(data)[i];
	}

	void setEmpty()
	{
		data = const_cast<char *>(reinterpret_cast<const char *>(&empty_auto_array_helper)) + sizeof(size_t);
	}

	void init(size_t size_, bool dont_init_elems)
	{
		if (!size_)
		{
			setEmpty();
			return;
		}
		
		data = new char[size_ * sizeof(T) + sizeof(size_t)];
		data += sizeof(size_t);
		m_size() = size_;

		if (!dont_init_elems)
			for (size_t i = 0; i < size_; ++i)
				new (place(i)) T();
	}

	void uninit()
	{
		size_t s = size();

		if (likely(s))
		{
			for (size_t i = 0; i < s; ++i)
				elem(i).~T();

			data -= sizeof(size_t);
			delete[] data;
		}
	}
};

}
