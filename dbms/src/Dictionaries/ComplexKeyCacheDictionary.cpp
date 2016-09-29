#include <DB/Dictionaries/ComplexKeyCacheDictionary.h>
#include <DB/Common/BitHelpers.h>
#include <DB/Common/randomSeed.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int TYPE_MISMATCH;
	extern const int BAD_ARGUMENTS;
	extern const int UNSUPPORTED_METHOD;
}


inline std::uint64_t ComplexKeyCacheDictionary::getCellIdx(const StringRef key) const
{
	const auto hash = StringRefHash{}(key);
	const auto idx = hash & (size - 1);
	return idx;
}


ComplexKeyCacheDictionary::ComplexKeyCacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
	DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
	const size_t size)
	: name{name}, dict_struct(dict_struct), source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
	size{roundUpToPowerOfTwoOrZero(size)}, rnd_engine{randomSeed()}
{
	if (!this->source_ptr->supportsSelectiveLoad())
		throw Exception{
			name + ": source cannot be used with ComplexKeyCacheDictionary",
			ErrorCodes::UNSUPPORTED_METHOD};

	createAttributes();
}

ComplexKeyCacheDictionary::ComplexKeyCacheDictionary(const ComplexKeyCacheDictionary & other)
	: ComplexKeyCacheDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.size}
{}


#define DECLARE(TYPE)\
void ComplexKeyCacheDictionary::get##TYPE(\
	const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
	PaddedPODArray<TYPE> & out) const\
{\
	dict_struct.validateKeyTypes(key_types);\
	\
	auto & attribute = getAttribute(attribute_name);\
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
		throw Exception{\
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
			ErrorCodes::TYPE_MISMATCH};\
	\
	const auto null_value = std::get<TYPE>(attribute.null_values);\
	\
	getItemsNumber<TYPE>(attribute, key_columns, out, [&] (const size_t) { return null_value; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void ComplexKeyCacheDictionary::getString(
	const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,
	ColumnString * out) const
{
	dict_struct.validateKeyTypes(key_types);

	auto & attribute = getAttribute(attribute_name);
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
		throw Exception{
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
			ErrorCodes::TYPE_MISMATCH};

	const auto null_value = StringRef{std::get<String>(attribute.null_values)};

	getItemsString(attribute, key_columns, out, [&] (const size_t) { return null_value; });
}

#define DECLARE(TYPE)\
void ComplexKeyCacheDictionary::get##TYPE(\
	const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
	const PaddedPODArray<TYPE> & def, PaddedPODArray<TYPE> & out) const\
{\
	dict_struct.validateKeyTypes(key_types);\
	\
	auto & attribute = getAttribute(attribute_name);\
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
		throw Exception{\
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
			ErrorCodes::TYPE_MISMATCH};\
	\
	getItemsNumber<TYPE>(attribute, key_columns, out, [&] (const size_t row) { return def[row]; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void ComplexKeyCacheDictionary::getString(
	const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,
	const ColumnString * const def, ColumnString * const out) const
{
	dict_struct.validateKeyTypes(key_types);

	auto & attribute = getAttribute(attribute_name);
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
		throw Exception{
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
			ErrorCodes::TYPE_MISMATCH};

	getItemsString(attribute, key_columns, out, [&] (const size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE)\
void ComplexKeyCacheDictionary::get##TYPE(\
	const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
	const TYPE def, PaddedPODArray<TYPE> & out) const\
{\
	dict_struct.validateKeyTypes(key_types);\
	\
	auto & attribute = getAttribute(attribute_name);\
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
		throw Exception{\
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
			ErrorCodes::TYPE_MISMATCH};\
	\
	getItemsNumber<TYPE>(attribute, key_columns, out, [&] (const size_t) { return def; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void ComplexKeyCacheDictionary::getString(
	const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,
	const String & def, ColumnString * const out) const
{
	dict_struct.validateKeyTypes(key_types);

	auto & attribute = getAttribute(attribute_name);
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
		throw Exception{
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
			ErrorCodes::TYPE_MISMATCH};

	getItemsString(attribute, key_columns, out, [&] (const size_t) { return StringRef{def}; });
}

void ComplexKeyCacheDictionary::has(const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const
{
	dict_struct.validateKeyTypes(key_types);

	/// Mapping: <key> -> { all indices `i` of `key_columns` such that `key_columns[i]` = <key> }
	MapType<std::vector<size_t>> outdated_keys;

	const auto rows = key_columns.front()->size();
	const auto keys_size = dict_struct.key.value().size();
	StringRefs keys(keys_size);
	Arena temporary_keys_pool;
	PODArray<StringRef> keys_array(rows);

	{
		const Poco::ScopedReadRWLock read_lock{rw_lock};

		const auto now = std::chrono::system_clock::now();
		/// fetch up-to-date values, decide which ones require update
		for (const auto row : ext::range(0, rows))
		{
			const StringRef key = placeKeysInPool(row, key_columns, keys, temporary_keys_pool);
			keys_array[row] = key;
			const auto hash = StringRefHash{}(key);
			const size_t cell_idx = hash & (size - 1);
			const auto & cell = cells[cell_idx];

			/** cell should be updated if either:
				*	1. keys (or hash) do not match,
				*	2. cell has expired,
				*	3. explicit defaults were specified and cell was set default. */
			if (cell.hash != hash || cell.key != key || cell.expiresAt() < now)
				outdated_keys[key].push_back(row);
			else
				out[row] = !cell.isDefault();
		}
	}

	query_count.fetch_add(rows, std::memory_order_relaxed);
	hit_count.fetch_add(rows - outdated_keys.size(), std::memory_order_release);

	if (outdated_keys.empty())
		return;

	std::vector<size_t> required_rows(outdated_keys.size());
	std::transform(std::begin(outdated_keys), std::end(outdated_keys), std::begin(required_rows),
		[] (auto & pair) { return pair.second.front(); });

	/// request new values
	update(key_columns, keys_array, required_rows,
		[&] (const StringRef key, const auto)
		{
			for (const auto out_idx : outdated_keys[key])
				out[out_idx] = true;
		},
		[&] (const StringRef key, const auto)
		{
			for (const auto out_idx : outdated_keys[key])
				out[out_idx] = false;
		});
}


void ComplexKeyCacheDictionary::createAttributes()
{
	const auto size = dict_struct.attributes.size();
	attributes.reserve(size);

	bytes_allocated += size * sizeof(CellMetadata);
	bytes_allocated += size * sizeof(attributes.front());

	for (const auto & attribute : dict_struct.attributes)
	{
		attribute_index_by_name.emplace(attribute.name, attributes.size());
		attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

		if (attribute.hierarchical)
			throw Exception{
				name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
				ErrorCodes::TYPE_MISMATCH};
	}
}

ComplexKeyCacheDictionary::Attribute ComplexKeyCacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
	Attribute attr{type};

	switch (type)
	{
		case AttributeUnderlyingType::UInt8:
			std::get<UInt8>(attr.null_values) = null_value.get<UInt64>();
			std::get<ContainerPtrType<UInt8>>(attr.arrays) = std::make_unique<ContainerType<UInt8>>(size);
			bytes_allocated += size * sizeof(UInt8);
			break;
		case AttributeUnderlyingType::UInt16:
			std::get<UInt16>(attr.null_values) = null_value.get<UInt64>();
			std::get<ContainerPtrType<UInt16>>(attr.arrays) = std::make_unique<ContainerType<UInt16>>(size);
			bytes_allocated += size * sizeof(UInt16);
			break;
		case AttributeUnderlyingType::UInt32:
			std::get<UInt32>(attr.null_values) = null_value.get<UInt64>();
			std::get<ContainerPtrType<UInt32>>(attr.arrays) = std::make_unique<ContainerType<UInt32>>(size);
			bytes_allocated += size * sizeof(UInt32);
			break;
		case AttributeUnderlyingType::UInt64:
			std::get<UInt64>(attr.null_values) = null_value.get<UInt64>();
			std::get<ContainerPtrType<UInt64>>(attr.arrays) = std::make_unique<ContainerType<UInt64>>(size);
			bytes_allocated += size * sizeof(UInt64);
			break;
		case AttributeUnderlyingType::Int8:
			std::get<Int8>(attr.null_values) = null_value.get<Int64>();
			std::get<ContainerPtrType<Int8>>(attr.arrays) = std::make_unique<ContainerType<Int8>>(size);
			bytes_allocated += size * sizeof(Int8);
			break;
		case AttributeUnderlyingType::Int16:
			std::get<Int16>(attr.null_values) = null_value.get<Int64>();
			std::get<ContainerPtrType<Int16>>(attr.arrays) = std::make_unique<ContainerType<Int16>>(size);
			bytes_allocated += size * sizeof(Int16);
			break;
		case AttributeUnderlyingType::Int32:
			std::get<Int32>(attr.null_values) = null_value.get<Int64>();
			std::get<ContainerPtrType<Int32>>(attr.arrays) = std::make_unique<ContainerType<Int32>>(size);
			bytes_allocated += size * sizeof(Int32);
			break;
		case AttributeUnderlyingType::Int64:
			std::get<Int64>(attr.null_values) = null_value.get<Int64>();
			std::get<ContainerPtrType<Int64>>(attr.arrays) = std::make_unique<ContainerType<Int64>>(size);
			bytes_allocated += size * sizeof(Int64);
			break;
		case AttributeUnderlyingType::Float32:
			std::get<Float32>(attr.null_values) = null_value.get<Float64>();
			std::get<ContainerPtrType<Float32>>(attr.arrays) = std::make_unique<ContainerType<Float32>>(size);
			bytes_allocated += size * sizeof(Float32);
			break;
		case AttributeUnderlyingType::Float64:
			std::get<Float64>(attr.null_values) = null_value.get<Float64>();
			std::get<ContainerPtrType<Float64>>(attr.arrays) = std::make_unique<ContainerType<Float64>>(size);
			bytes_allocated += size * sizeof(Float64);
			break;
		case AttributeUnderlyingType::String:
			std::get<String>(attr.null_values) = null_value.get<String>();
			std::get<ContainerPtrType<StringRef>>(attr.arrays) = std::make_unique<ContainerType<StringRef>>(size);
			bytes_allocated += size * sizeof(StringRef);
			if (!string_arena)
				string_arena = std::make_unique<ArenaWithFreeLists>();
			break;
	}

	return attr;
}

template <typename OutputType, typename DefaultGetter>
void ComplexKeyCacheDictionary::getItemsNumber(
	Attribute & attribute,
	const ConstColumnPlainPtrs & key_columns,
	PaddedPODArray<OutputType> & out,
	DefaultGetter && get_default) const
{
	if (false) {}
#define DISPATCH(TYPE) \
	else if (attribute.type == AttributeUnderlyingType::TYPE) \
		getItemsNumberImpl<TYPE, OutputType>(attribute, key_columns, out, std::forward<DefaultGetter>(get_default));
	DISPATCH(UInt8)
	DISPATCH(UInt16)
	DISPATCH(UInt32)
	DISPATCH(UInt64)
	DISPATCH(Int8)
	DISPATCH(Int16)
	DISPATCH(Int32)
	DISPATCH(Int64)
	DISPATCH(Float32)
	DISPATCH(Float64)
#undef DISPATCH
	else
		throw Exception("Unexpected type of attribute: " + toString(attribute.type), ErrorCodes::LOGICAL_ERROR);
}

template <typename AttributeType, typename OutputType, typename DefaultGetter>
void ComplexKeyCacheDictionary::getItemsNumberImpl(
	Attribute & attribute,
	const ConstColumnPlainPtrs & key_columns,
	PaddedPODArray<OutputType> & out,
	DefaultGetter && get_default) const
{
	/// Mapping: <key> -> { all indices `i` of `key_columns` such that `key_columns[i]` = <key> }
	MapType<std::vector<size_t>> outdated_keys;
	auto & attribute_array = std::get<ContainerPtrType<AttributeType>>(attribute.arrays);

	const auto rows = key_columns.front()->size();
	const auto keys_size = dict_struct.key.value().size();
	StringRefs keys(keys_size);
	Arena temporary_keys_pool;
	PODArray<StringRef> keys_array(rows);

	{
		const Poco::ScopedReadRWLock read_lock{rw_lock};

		const auto now = std::chrono::system_clock::now();
		/// fetch up-to-date values, decide which ones require update
		for (const auto row : ext::range(0, rows))
		{
			const StringRef key = placeKeysInPool(row, key_columns, keys, temporary_keys_pool);
			keys_array[row] = key;
			const auto hash = StringRefHash{}(key);
			const size_t cell_idx = hash & (size - 1);
			const auto & cell = cells[cell_idx];

			/** cell should be updated if either:
				*	1. keys (or hash) do not match,
				*	2. cell has expired,
				*	3. explicit defaults were specified and cell was set default. */
			if (cell.hash != hash || cell.key != key || cell.expiresAt() < now)
				outdated_keys[key].push_back(row);
			else
				out[row] =  cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
		}
	}

	query_count.fetch_add(rows, std::memory_order_relaxed);
	hit_count.fetch_add(rows - outdated_keys.size(), std::memory_order_release);

	if (outdated_keys.empty())
		return;

	std::vector<size_t> required_rows(outdated_keys.size());
	std::transform(std::begin(outdated_keys), std::end(outdated_keys), std::begin(required_rows),
		[] (auto & pair) { return pair.second.front(); });

	/// request new values
	update(key_columns, keys_array, required_rows,
		[&] (const StringRef key, const size_t cell_idx)
		{
			for (const auto row : outdated_keys[key])
				out[row] = attribute_array[cell_idx];
		},
		[&] (const StringRef key, const size_t cell_idx)
		{
			for (const auto row : outdated_keys[key])
				out[row] = get_default(row);
		});
}


template <typename DefaultGetter>
void ComplexKeyCacheDictionary::getItemsString(
	Attribute & attribute, const ConstColumnPlainPtrs & key_columns, ColumnString * out,
	DefaultGetter && get_default) const
{
	const auto rows = key_columns.front()->size();
	/// save on some allocations
	out->getOffsets().reserve(rows);

	const auto keys_size = dict_struct.key.value().size();
	StringRefs keys(keys_size);
	Arena temporary_keys_pool;

	auto & attribute_array = std::get<ContainerPtrType<StringRef>>(attribute.arrays);

	auto found_outdated_values = false;

	/// perform optimistic version, fallback to pessimistic if failed
	{
		const Poco::ScopedReadRWLock read_lock{rw_lock};

		const auto now = std::chrono::system_clock::now();
		/// fetch up-to-date values, discard on fail
		for (const auto row : ext::range(0, rows))
		{
			const StringRef key = placeKeysInPool(row, key_columns, keys, temporary_keys_pool);
			SCOPE_EXIT(temporary_keys_pool.rollback(key.size));
			const auto hash = StringRefHash{}(key);
			const size_t cell_idx = hash & (size - 1);
			const auto & cell = cells[cell_idx];

			if (cell.hash != hash || cell.key != key || cell.expiresAt() < now)
			{
				found_outdated_values = true;
				break;
			}
			else
			{
				const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
				out->insertData(string_ref.data, string_ref.size);
			}
		}
	}

	/// optimistic code completed successfully
	if (!found_outdated_values)
	{
		query_count.fetch_add(rows, std::memory_order_relaxed);
		hit_count.fetch_add(rows, std::memory_order_release);
		return;
	}

	/// now onto the pessimistic one, discard possible partial results from the optimistic path
	out->getChars().resize_assume_reserved(0);
	out->getOffsets().resize_assume_reserved(0);

	/// Mapping: <key> -> { all indices `i` of `key_columns` such that `key_columns[i]` = <key> }
	MapType<std::vector<size_t>> outdated_keys;
	/// we are going to store every string separately
	MapType<StringRef> map;
	PODArray<StringRef> keys_array(rows);

	size_t total_length = 0;
	{
		const Poco::ScopedReadRWLock read_lock{rw_lock};

		const auto now = std::chrono::system_clock::now();
		for (const auto row : ext::range(0, rows))
		{
			const StringRef key = placeKeysInPool(row, key_columns, keys, temporary_keys_pool);
			keys_array[row] = key;
			const auto hash = StringRefHash{}(key);
			const size_t cell_idx = hash & (size - 1);
			const auto & cell = cells[cell_idx];

			if (cell.hash != hash || cell.key != key || cell.expiresAt() < now)
				outdated_keys[key].push_back(row);
			else
			{
				const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

				if (!cell.isDefault())
					map[key] = copyIntoArena(string_ref, temporary_keys_pool);

				total_length += string_ref.size + 1;
			}
		}
	}

	query_count.fetch_add(rows, std::memory_order_relaxed);
	hit_count.fetch_add(rows - outdated_keys.size(), std::memory_order_release);

	/// request new values
	if (!outdated_keys.empty())
	{
		std::vector<size_t> required_rows(outdated_keys.size());
		std::transform(std::begin(outdated_keys), std::end(outdated_keys), std::begin(required_rows),
			[] (auto & pair) { return pair.second.front(); });

		update(key_columns, keys_array, required_rows,
			[&] (const StringRef key, const size_t cell_idx)
			{
				const StringRef attribute_value = attribute_array[cell_idx];

				/// We must copy key and value to own memory, because it may be replaced with another
				///  in next iterations of inner loop of update.
				const StringRef copied_key = copyIntoArena(key, temporary_keys_pool);
				const StringRef copied_value = copyIntoArena(attribute_value, temporary_keys_pool);

				map[copied_key] = copied_value;
				total_length += (attribute_value.size + 1) * outdated_keys[key].size();
			},
			[&] (const StringRef key, const size_t cell_idx)
			{
				for (const auto row : outdated_keys[key])
					total_length += get_default(row).size + 1;
			});
	}

	out->getChars().reserve(total_length);

	for (const auto row : ext::range(0, ext::size(keys_array)))
	{
		const StringRef key = keys_array[row];
		const auto it = map.find(key);
		const auto string_ref = it != std::end(map) ? it->second : get_default(row);
		out->insertData(string_ref.data, string_ref.size);
	}
}

template <typename PresentKeyHandler, typename AbsentKeyHandler>
void ComplexKeyCacheDictionary::update(
	const ConstColumnPlainPtrs & in_key_columns, const PODArray<StringRef> & in_keys,
	const std::vector<size_t> & in_requested_rows, PresentKeyHandler && on_cell_updated,
	AbsentKeyHandler && on_key_not_found) const
{
	MapType<bool> remaining_keys{in_requested_rows.size()};
	for (const auto row : in_requested_rows)
		remaining_keys.insert({ in_keys[row], false });

	std::uniform_int_distribution<std::uint64_t> distribution{
		dict_lifetime.min_sec,
		dict_lifetime.max_sec
	};

	const Poco::ScopedWriteRWLock write_lock{rw_lock};

	auto stream = source_ptr->loadKeys(in_key_columns, in_requested_rows);
	stream->readPrefix();

	const auto keys_size = dict_struct.key.value().size();
	StringRefs keys(keys_size);

	const auto attributes_size = attributes.size();

	while (const auto block = stream->read())
	{
		/// cache column pointers
		const auto key_columns = ext::map<ConstColumnPlainPtrs>(
			ext::range(0, keys_size),
			[&] (const size_t attribute_idx)
			{
				return block.getByPosition(attribute_idx).column.get();
			});

		const auto attribute_columns = ext::map<ConstColumnPlainPtrs>(
			ext::range(0, attributes_size),
			[&] (const size_t attribute_idx)
			{
				return block.getByPosition(keys_size + attribute_idx).column.get();
			});

		const auto rows = block.rowsInFirstColumn();

		for (const auto row : ext::range(0, rows))
		{
			auto key = allocKey(row, key_columns, keys);
			const auto hash = StringRefHash{}(key);
			const size_t cell_idx = hash & (size - 1);
			auto & cell = cells[cell_idx];

			for (const auto attribute_idx : ext::range(0, attributes.size()))
			{
				const auto & attribute_column = *attribute_columns[attribute_idx];
				auto & attribute = attributes[attribute_idx];

				setAttributeValue(attribute, cell_idx, attribute_column[row]);
			}

			/// if cell id is zero and zero does not map to this cell, then the cell is unused
			if (cell.key == StringRef{} && cell_idx != zero_cell_idx)
				element_count.fetch_add(1, std::memory_order_relaxed);

			/// handle memory allocated for old key
			if (key == cell.key)
			{
				freeKey(key);
				key = cell.key;
			}
			else
			{
				/// new key is different from the old one
				if (cell.key.data)
					freeKey(cell.key);

				cell.key = key;
			}

			cell.hash = hash;

			if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
				cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
			else
				cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

			/// inform caller
			on_cell_updated(key, cell_idx);
			/// mark corresponding id as found
			remaining_keys[key] = true;
		}
	}

	stream->readSuffix();

	/// Check which ids have not been found and require setting null_value
	for (const auto key_found_pair : remaining_keys)
	{
		if (key_found_pair.second)
			continue;

		auto key = key_found_pair.first;
		const auto hash = StringRefHash{}(key);
		const size_t cell_idx = hash & (size - 1);
		auto & cell = cells[cell_idx];

		/// Set null_value for each attribute
		for (auto & attribute : attributes)
			setDefaultAttributeValue(attribute, cell_idx);

		/// Check if cell had not been occupied before and increment element counter if it hadn't
		if (cell.key == StringRef{} && cell_idx != zero_cell_idx)
			element_count.fetch_add(1, std::memory_order_relaxed);

		if (key == cell.key)
			key = cell.key;
		else
		{
			if (cell.key.data)
				freeKey(cell.key);

			/// copy key from temporary pool
			key = copyKey(key);
			cell.key = key;
		}

		cell.hash = hash;

		if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
			cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
		else
			cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

		cell.setDefault();

		/// inform caller that the cell has not been found
		on_key_not_found(key, cell_idx);
	}
}


void ComplexKeyCacheDictionary::setDefaultAttributeValue(Attribute & attribute, const size_t idx) const
{
	switch (attribute.type)
	{
		case AttributeUnderlyingType::UInt8: std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = std::get<UInt8>(attribute.null_values); break;
		case AttributeUnderlyingType::UInt16: std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = std::get<UInt16>(attribute.null_values); break;
		case AttributeUnderlyingType::UInt32: std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = std::get<UInt32>(attribute.null_values); break;
		case AttributeUnderlyingType::UInt64: std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = std::get<UInt64>(attribute.null_values); break;
		case AttributeUnderlyingType::Int8: std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = std::get<Int8>(attribute.null_values); break;
		case AttributeUnderlyingType::Int16: std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = std::get<Int16>(attribute.null_values); break;
		case AttributeUnderlyingType::Int32: std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = std::get<Int32>(attribute.null_values); break;
		case AttributeUnderlyingType::Int64: std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = std::get<Int64>(attribute.null_values); break;
		case AttributeUnderlyingType::Float32: std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = std::get<Float32>(attribute.null_values); break;
		case AttributeUnderlyingType::Float64: std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = std::get<Float64>(attribute.null_values); break;
		case AttributeUnderlyingType::String:
		{
			const auto & null_value_ref = std::get<String>(attribute.null_values);
			auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];

			if (string_ref.data != null_value_ref.data())
			{
				if (string_ref.data)
					string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

				string_ref = StringRef{null_value_ref};
			}

			break;
		}
	}
}

void ComplexKeyCacheDictionary::setAttributeValue(Attribute & attribute, const size_t idx, const Field & value) const
{
	switch (attribute.type)
	{
		case AttributeUnderlyingType::UInt8: std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
		case AttributeUnderlyingType::UInt16: std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
		case AttributeUnderlyingType::UInt32: std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
		case AttributeUnderlyingType::UInt64: std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
		case AttributeUnderlyingType::Int8: std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = value.get<Int64>(); break;
		case AttributeUnderlyingType::Int16: std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = value.get<Int64>(); break;
		case AttributeUnderlyingType::Int32: std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = value.get<Int64>(); break;
		case AttributeUnderlyingType::Int64: std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = value.get<Int64>(); break;
		case AttributeUnderlyingType::Float32: std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = value.get<Float64>(); break;
		case AttributeUnderlyingType::Float64: std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = value.get<Float64>(); break;
		case AttributeUnderlyingType::String:
		{
			const auto & string = value.get<String>();
			auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];
			const auto & null_value_ref = std::get<String>(attribute.null_values);

			/// free memory unless it points to a null_value
			if (string_ref.data && string_ref.data != null_value_ref.data())
				string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

			const auto size = string.size();
			if (size != 0)
			{
				auto string_ptr = string_arena->alloc(size + 1);
				std::copy(string.data(), string.data() + size + 1, string_ptr);
				string_ref = StringRef{string_ptr, size};
			}
			else
				string_ref = {};

			break;
		}
	}
}

ComplexKeyCacheDictionary::Attribute & ComplexKeyCacheDictionary::getAttribute(const std::string & attribute_name) const
{
	const auto it = attribute_index_by_name.find(attribute_name);
	if (it == std::end(attribute_index_by_name))
		throw Exception{
			name + ": no such attribute '" + attribute_name + "'",
			ErrorCodes::BAD_ARGUMENTS};

	return attributes[it->second];
}

StringRef ComplexKeyCacheDictionary::allocKey(const size_t row, const ConstColumnPlainPtrs & key_columns, StringRefs & keys) const
{
	if (key_size_is_fixed)
		return placeKeysInFixedSizePool(row, key_columns);

	return placeKeysInPool(row, key_columns, keys, *keys_pool);
}

void ComplexKeyCacheDictionary::freeKey(const StringRef key) const
{
	if (key_size_is_fixed)
		fixed_size_keys_pool->free(const_cast<char *>(key.data));
	else
		keys_pool->free(const_cast<char *>(key.data), key.size);
}

template <typename Arena>
StringRef ComplexKeyCacheDictionary::placeKeysInPool(
	const size_t row, const ConstColumnPlainPtrs & key_columns, StringRefs & keys, Arena & pool)
{
	const auto keys_size = key_columns.size();
	size_t sum_keys_size{};
	for (const auto i : ext::range(0, keys_size))
	{
		keys[i] = key_columns[i]->getDataAtWithTerminatingZero(row);
		sum_keys_size += keys[i].size;
	}

	const auto res = pool.alloc(sum_keys_size);
	auto place = res;

	for (size_t j = 0; j < keys_size; ++j)
	{
		memcpy(place, keys[j].data, keys[j].size);
		place += keys[j].size;
	}

	return { res, sum_keys_size };
}

StringRef ComplexKeyCacheDictionary::placeKeysInFixedSizePool(
	const size_t row, const ConstColumnPlainPtrs & key_columns) const
{
	const auto res = fixed_size_keys_pool->alloc();
	auto place = res;

	for (const auto & key_column : key_columns)
	{
		const StringRef key = key_column->getDataAt(row);
		memcpy(place, key.data, key.size);
		place += key.size;
	}

	return { res, key_size };
}

StringRef ComplexKeyCacheDictionary::copyIntoArena(StringRef src, Arena & arena)
{
	char * allocated = arena.alloc(src.size);
	memcpy(allocated, src.data, src.size);
	return { allocated, src.size };
}

StringRef ComplexKeyCacheDictionary::copyKey(const StringRef key) const
{
	const auto res = key_size_is_fixed ? fixed_size_keys_pool->alloc() : keys_pool->alloc(key.size);
	memcpy(res, key.data, key.size);

	return { res, key.size };
}

}
