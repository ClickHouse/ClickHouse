#pragma once

#include <typeinfo>
#include <Poco/Exception.h>
#include <DB/Core/StringRef.h>
#include <common/Common.h>

#define PURE __attribute__((pure))


/** Очень простой класс для чтения JSON (или его кусочков).
  * Представляет собой ссылку на кусок памяти, в котором содержится JSON (или его кусочек).
  * Не создаёт никаких структур данных в оперативке. Не выделяет память (кроме std::string).
  * Не парсит JSON до конца (парсит только часть, необходимую для выполнения вызванного метода).
  * Парсинг необходимой части запускается каждый раз при вызове методов.
  * Может работать с обрезанным JSON-ом.
  * При этом, (в отличие от SAX-подобных парсеров), предоставляет удобные методы для работы. 
  *
  * Эта структура данных более оптимальна, если нужно доставать несколько элементов из большого количества маленьких JSON-ов.
  * То есть, подходит для обработки "параметров визитов" и "параметров интернет магазинов" в Яндекс.Метрике.
  * Если нужно много работать с одним большим JSON-ом, то этот класс может быть менее оптимальным.
  *
  * Имеются следующие соглашения:
  * 1. Предполагается, что в JSON-е нет пробельных символов.
  * 2. Предполагается, что строки в JSON в кодировке UTF-8; также могут использоваться \u-последовательности.
  *    Строки возвращаются в кодировке UTF-8, \u-последовательности переводятся в UTF-8.
  * 3. Но суррогатная пара из двух \uXXXX\uYYYY переводится не в UTF-8, а в CESU-8.
  * 4. Корректный JSON парсится корректно.
  *    При работе с некорректным JSON-ом, кидается исключение или возвращаются неверные результаты.
  *    (пример: считается, что если встретился символ 'n', то после него идёт 'ull' (null);
  *     если после него идёт ',1,', то исключение не кидается, и, таким образом, возвращается неверный результат)
  * 5. Глубина вложенности JSON ограничена (см. MAX_JSON_DEPTH в cpp файле).
  *    При необходимости спуститься на большую глубину, кидается исключение.
  * 6. В отличие от JSON, пользоволяет парсить значения вида 64-битное число, со знаком, или без.
  *    При этом, если число дробное - то дробная часть тихо отбрасывается.
  * 7. Числа с плавающей запятой парсятся не с максимальной точностью.
  *
  * Подходит только для чтения JSON, модификация не предусмотрена.
  * Все методы immutable, кроме operator++.
  */


POCO_DECLARE_EXCEPTION(Foundation_API, JSONException, Poco::Exception);


class JSON
{
private:
	typedef const char * Pos;
	Pos ptr_begin;
	Pos ptr_end;
	unsigned level;

public:
	JSON(Pos ptr_begin_, Pos ptr_end_, unsigned level_ = 0) : ptr_begin(ptr_begin_), ptr_end(ptr_end_), level(level_)
	{
		checkInit();
	}

	JSON(const std::string & s) : ptr_begin(s.data()), ptr_end(s.data() + s.size()), level(0)
	{
		checkInit();
	}

	JSON(const JSON & rhs) : ptr_begin(rhs.ptr_begin), ptr_end(rhs.ptr_end), level(rhs.level) {}

	/// Для вставки в контейнеры (создаёт некорректный объект)
	JSON() : ptr_begin(nullptr), ptr_end(ptr_begin + 1) {}

	const char * data() const PURE { return ptr_begin; }
	const char * dataEnd() const PURE { return ptr_end; }

	enum ElementType
	{
		TYPE_OBJECT,
		TYPE_ARRAY,
		TYPE_NUMBER,
		TYPE_STRING,
		TYPE_BOOL,
		TYPE_NULL,
		TYPE_NAME_VALUE_PAIR,
		TYPE_NOTYPE,
	};

	ElementType getType() const PURE;
	
	bool isObject() const PURE			{ return getType() == TYPE_OBJECT; };
	bool isArray() const PURE			{ return getType() == TYPE_ARRAY; };
	bool isNumber() const PURE			{ return getType() == TYPE_NUMBER; };
	bool isString() const PURE			{ return getType() == TYPE_STRING; };
	bool isBool() const PURE			{ return getType() == TYPE_BOOL; };
	bool isNull() const PURE			{ return getType() == TYPE_NULL; };
	bool isNameValuePair() const PURE	{ return getType() == TYPE_NAME_VALUE_PAIR; };

	/// Количество элементов в массиве или объекте; если элемент - не массив или объект, то исключение.
	size_t size() const PURE;

	/// Является ли массив или объект пустыми; если элемент - не массив или объект, то исключение.
	bool empty() const PURE;

	/// Получить элемент массива по индексу; если элемент - не массив, то исключение.
	JSON operator[] (size_t n) const PURE;

	/// Получить элемент объекта по имени; если элемент - не объект, то исключение.
	JSON operator[] (const std::string & name) const PURE;

	/// Есть ли в объекте элемент с заданным именем; если элемент - не объект, то исключение.
	bool has(const std::string & name) const PURE { return has(name.data(), name.size()); }
	bool has(const char * data, size_t size) const PURE;

	/// Получить значение элемента; исключение, если элемент имеет неправильный тип.
	template <class T>
	T get() const PURE;

	/// если значения нет, или тип неверный, то возвращает дефолтное значение
	template <class T>
	T getWithDefault(const std::string & key, const T & default_ = T()) const PURE;

	double 		getDouble() const PURE;
	Int64 		getInt() const PURE;	/// Отбросить дробную часть.
	UInt64 		getUInt() const PURE;	/// Отбросить дробную часть. Если число отрицательное - исключение.
	std::string getString() const PURE;
	bool 		getBool() const PURE;
	std::string getName() const PURE;	/// Получить имя name-value пары.
	JSON		getValue() const PURE;	/// Получить значение name-value пары.

	StringRef getRawString() const PURE;
	StringRef getRawName() const PURE;

	/// Получить значение элемента; если элемент - строка, то распарсить значение из строки; если не строка или число - то исключение.
	double 		toDouble() const PURE;
	Int64 		toInt() const PURE;
	UInt64 		toUInt() const PURE;

	/** Преобразовать любой элемент в строку.
	  * Для строки возвращается её значение, для всех остальных элементов - сериализованное представление.
	  */
	std::string toString() const PURE;

	/// Класс JSON одновременно является итератором по самому себе.
	typedef JSON iterator;
	typedef JSON const_iterator;
	
	iterator operator* () const PURE { return *this; }
	const JSON * operator-> () const PURE { return this; }
	bool operator== (const JSON & rhs) const PURE { return ptr_begin == rhs.ptr_begin; }
	bool operator!= (const JSON & rhs) const PURE { return ptr_begin != rhs.ptr_begin; }

	/** Если элемент - массив или объект, то begin() возвращает iterator,
	  * который указывает на первый элемент массива или первую name-value пару объекта.
	  */
	iterator begin() const PURE;

	/** end() - значение, которое нельзя использовать; сигнализирует о том, что элементы закончились.
	  */
	iterator end() const PURE;

	/// Перейти к следующему элементу массива или следующей name-value паре объекта.
	iterator & operator++();
	iterator operator++(int);

	/// Есть ли в строке escape-последовательности
	bool hasEscapes() const PURE;

	/// Есть ли в строке спец-символы из набора \, ', \0, \b, \f, \r, \n, \t, возможно, заэскейпленные.
	bool hasSpecialChars() const PURE;

private:
	/// Проверить глубину рекурсии, а также корректность диапазона памяти.
	void checkInit() const PURE;
	/// Проверить, что pos лежит внутри диапазона памяти.
	void checkPos(Pos pos) const PURE;

	/// Вернуть позицию после заданного элемента.
	Pos skipString() const PURE;
	Pos skipNumber() const PURE;
	Pos skipBool() const PURE;
	Pos skipNull() const PURE;
	Pos skipNameValuePair() const PURE;
	Pos skipObject() const PURE;
	Pos skipArray() const PURE;

	Pos skipElement() const PURE;

	/// Найти name-value пару с заданным именем в объекте.
	Pos searchField(const std::string & name) const PURE { return searchField(name.data(), name.size()); }
	Pos searchField(const char * data, size_t size) const PURE;

	template <class T>
	bool isType() const PURE;
};

template <class T>
T JSON::getWithDefault(const std::string & key, const T & default_) const
{
	if (has(key))
	{
		JSON key_json = (*this)[key];

		if (key_json.isType<T>())
			return key_json.get<T>();
		else
			return default_;
	}
	else
		return default_;
}

#undef PURE
