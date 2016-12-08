#include <DB/Columns/ColumnsNumber.h>
#include <DB/Dictionaries/CacheDictionary.h>
#include <DB/Common/BitHelpers.h>
#include <DB/Common/randomSeed.h>
#include <DB/Common/HashTable/Hash.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int TYPE_MISMATCH;
	extern const int BAD_ARGUMENTS;
	extern const int UNSUPPORTED_METHOD;
}


inline UInt64 CacheDictionary::getCellIdx(const Key id) const
{
	const auto hash = intHash64(id);
	const auto idx = hash & (size - 1);
	return idx;
}


CacheDictionary::CacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
	DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
	const std::size_t size)
	: name{name}, dict_struct(dict_struct),
		source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
		size{roundUpToPowerOfTwoOrZero(size)},
		cells{this->size},
		rnd_engine{randomSeed()}
{
	if (!this->source_ptr->supportsSelectiveLoad())
		throw Exception{
			name + ": source cannot be used with CacheDictionary",
			ErrorCodes::UNSUPPORTED_METHOD};

	createAttributes();
}

CacheDictionary::CacheDictionary(const CacheDictionary & other)
	: CacheDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.size}
{}


void CacheDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
{
	const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

	getItemsNumber<UInt64>(*hierarchical_attribute, ids, out, [&] (const std::size_t) { return null_value; });
}


#define DECLARE(TYPE)\
void CacheDictionary::get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, PaddedPODArray<TYPE> & out) const\
{\
	auto & attribute = getAttribute(attribute_name);\
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
		throw Exception{\
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
			ErrorCodes::TYPE_MISMATCH};\
	\
	const auto null_value = std::get<TYPE>(attribute.null_values);\
	\
	getItemsNumber<TYPE>(attribute, ids, out, [&] (const std::size_t) { return null_value; });\
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

void CacheDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
	auto & attribute = getAttribute(attribute_name);
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
		throw Exception{
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
			ErrorCodes::TYPE_MISMATCH};

	const auto null_value = StringRef{std::get<String>(attribute.null_values)};

	getItemsString(attribute, ids, out, [&] (const std::size_t) { return null_value; });
}

#define DECLARE(TYPE)\
void CacheDictionary::get##TYPE(\
	const std::string & attribute_name, const PaddedPODArray<Key> & ids, const PaddedPODArray<TYPE> & def,\
	PaddedPODArray<TYPE> & out) const\
{\
	auto & attribute = getAttribute(attribute_name);\
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
		throw Exception{\
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
			ErrorCodes::TYPE_MISMATCH};\
	\
	getItemsNumber<TYPE>(attribute, ids, out, [&] (const std::size_t row) { return def[row]; });\
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

void CacheDictionary::getString(
	const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def,
	ColumnString * const out) const
{
	auto & attribute = getAttribute(attribute_name);
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
		throw Exception{
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
			ErrorCodes::TYPE_MISMATCH};

	getItemsString(attribute, ids, out, [&] (const std::size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE)\
void CacheDictionary::get##TYPE(\
	const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def, PaddedPODArray<TYPE> & out) const\
{\
	auto & attribute = getAttribute(attribute_name);\
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
		throw Exception{\
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
			ErrorCodes::TYPE_MISMATCH};\
	\
	getItemsNumber<TYPE>(attribute, ids, out, [&] (const std::size_t) { return def; });\
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

void CacheDictionary::getString(
	const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def,
	ColumnString * const out) const
{
	auto & attribute = getAttribute(attribute_name);
	if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
		throw Exception{
			name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
			ErrorCodes::TYPE_MISMATCH};

	getItemsString(attribute, ids, out, [&] (const std::size_t) { return StringRef{def}; });
}


void CacheDictionary::has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
	/// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
	std::unordered_map<Key, std::vector<std::size_t>> outdated_ids;

	const auto rows = ext::size(ids);
	{
		const Poco::ScopedReadRWLock read_lock{rw_lock};

		const auto now = std::chrono::system_clock::now();
		/// fetch up-to-date values, decide which ones require update
		for (const auto row : ext::range(0, rows))
		{
			const auto id = ids[row];
			const auto cell_idx = getCellIdx(id);
			const auto & cell = cells[cell_idx];

			/** cell should be updated if either:
				*	1. ids do not match,
				*	2. cell has expired,
				*	3. explicit defaults were specified and cell was set default. */
			if (cell.id != id || cell.expiresAt() < now)
				outdated_ids[id].push_back(row);
			else
				out[row] = !cell.isDefault();
		}
	}

	query_count.fetch_add(rows, std::memory_order_relaxed);
	hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

	if (outdated_ids.empty())
		return;

	std::vector<Key> required_ids(outdated_ids.size());
	std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
		[] (auto & pair) { return pair.first; });

	/// request new values
	update(required_ids, [&] (const auto id, const auto) {
		for (const auto row : outdated_ids[id])
			out[row] = true;
	}, [&] (const auto id, const auto) {
		for (const auto row : outdated_ids[id])
			out[row] = false;
	});
}


void CacheDictionary::createAttributes()
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
		{
			hierarchical_attribute = &attributes.back();

			if (hierarchical_attribute->type != AttributeUnderlyingType::UInt64)
				throw Exception{
					name + ": hierarchical attribute must be UInt64.",
					ErrorCodes::TYPE_MISMATCH};
		}
	}
}

CacheDictionary::Attribute CacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
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
void CacheDictionary::getItemsNumber(
	Attribute & attribute,
	const PaddedPODArray<Key> & ids,
	PaddedPODArray<OutputType> & out,
	DefaultGetter && get_default) const
{
	if (false) {}
#define DISPATCH(TYPE) \
	else if (attribute.type == AttributeUnderlyingType::TYPE) \
		getItemsNumberImpl<TYPE, OutputType>(attribute, ids, out, std::forward<DefaultGetter>(get_default));
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
void CacheDictionary::getItemsNumberImpl(
	Attribute & attribute,
	const PaddedPODArray<Key> & ids,
	PaddedPODArray<OutputType> & out,
	DefaultGetter && get_default) const
{
	/// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
	std::unordered_map<Key, std::vector<std::size_t>> outdated_ids;
	auto & attribute_array = std::get<ContainerPtrType<AttributeType>>(attribute.arrays);
	const auto rows = ext::size(ids);

	{
		const Poco::ScopedReadRWLock read_lock{rw_lock};

		const auto now = std::chrono::system_clock::now();
		/// fetch up-to-date values, decide which ones require update
		for (const auto row : ext::range(0, rows))
		{
			const auto id = ids[row];
			const auto cell_idx = getCellIdx(id);
			const auto & cell = cells[cell_idx];

			/** cell should be updated if either:
				*	1. ids do not match,
				*	2. cell has expired,
				*	3. explicit defaults were specified and cell was set default. */
			if (cell.id != id || cell.expiresAt() < now)
				outdated_ids[id].push_back(row);
			else
				out[row] = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
		}
	}

	query_count.fetch_add(rows, std::memory_order_relaxed);
	hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

	if (outdated_ids.empty())
		return;

	std::vector<Key> required_ids(outdated_ids.size());
	std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
		[] (auto & pair) { return pair.first; });

	/// request new values
	update(required_ids, [&] (const auto id, const auto cell_idx) {
		const auto attribute_value = attribute_array[cell_idx];

		for (const auto row : outdated_ids[id])
			out[row] = attribute_value;
	}, [&] (const auto id, const auto cell_idx) {
		for (const auto row : outdated_ids[id])
			out[row] = get_default(row);
	});
}

template <typename DefaultGetter>
void CacheDictionary::getItemsString(
	Attribute & attribute,
	const PaddedPODArray<Key> & ids,
	ColumnString * out,
	DefaultGetter && get_default) const
{
	const auto rows = ext::size(ids);

	/// save on some allocations
	out->getOffsets().reserve(rows);

	auto & attribute_array = std::get<ContainerPtrType<StringRef>>(attribute.arrays);

	auto found_outdated_values = false;

	/// perform optimistic version, fallback to pessimistic if failed
	{
		const Poco::ScopedReadRWLock read_lock{rw_lock};

		const auto now = std::chrono::system_clock::now();
		/// fetch up-to-date values, discard on fail
		for (const auto row : ext::range(0, rows))
		{
			const auto id = ids[row];
			const auto cell_idx = getCellIdx(id);
			const auto & cell = cells[cell_idx];

			if (cell.id != id || cell.expiresAt() < now)
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

	/// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
	std::unordered_map<Key, std::vector<std::size_t>> outdated_ids;
	/// we are going to store every string separately
	std::unordered_map<Key, String> map;

	std::size_t total_length = 0;
	{
		const Poco::ScopedReadRWLock read_lock{rw_lock};

		const auto now = std::chrono::system_clock::now();
		for (const auto row : ext::range(0, ids.size()))
		{
			const auto id = ids[row];
			const auto cell_idx = getCellIdx(id);
			const auto & cell = cells[cell_idx];

			if (cell.id != id || cell.expiresAt() < now)
				outdated_ids[id].push_back(row);
			else
			{
				const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

				if (!cell.isDefault())
					map[id] = String{string_ref};

				total_length += string_ref.size + 1;
			}
		}
	}

	query_count.fetch_add(rows, std::memory_order_relaxed);
	hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

	/// request new values
	if (!outdated_ids.empty())
	{
		std::vector<Key> required_ids(outdated_ids.size());
		std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
			[] (auto & pair) { return pair.first; });

		update(required_ids, [&] (const auto id, const auto cell_idx) {
			const auto attribute_value = attribute_array[cell_idx];

			map[id] = String{attribute_value};
			total_length += (attribute_value.size + 1) * outdated_ids[id].size();
		}, [&] (const auto id, const auto cell_idx) {
			for (const auto row : outdated_ids[id])
				total_length += get_default(row).size + 1;
		});
	}

	out->getChars().reserve(total_length);

	for (const auto row : ext::range(0, ext::size(ids)))
	{
		const auto id = ids[row];
		const auto it = map.find(id);

		const auto string_ref = it != std::end(map) ? StringRef{it->second} : get_default(row);
		out->insertData(string_ref.data, string_ref.size);
	}
}

template <typename PresentIdHandler, typename AbsentIdHandler>
void CacheDictionary::update(
	const std::vector<Key> & requested_ids, PresentIdHandler && on_cell_updated,
	AbsentIdHandler && on_id_not_found) const
{
	std::unordered_map<Key, UInt8> remaining_ids{requested_ids.size()};
	for (const auto id : requested_ids)
		remaining_ids.insert({ id, 0 });

	std::uniform_int_distribution<UInt64> distribution{
		dict_lifetime.min_sec,
		dict_lifetime.max_sec
	};

	const Poco::ScopedWriteRWLock write_lock{rw_lock};

	auto stream = source_ptr->loadIds(requested_ids);
	stream->readPrefix();

	while (const auto block = stream->read())
	{
		const auto id_column = typeid_cast<const ColumnUInt64 *>(block.getByPosition(0).column.get());
		if (!id_column)
			throw Exception{
				name + ": id column has type different from UInt64.",
				ErrorCodes::TYPE_MISMATCH};

		const auto & ids = id_column->getData();

		/// cache column pointers
		const auto column_ptrs = ext::map<std::vector>(ext::range(0, attributes.size()), [&block] (const auto & i) {
			return block.getByPosition(i + 1).column.get();
		});

		for (const auto i : ext::range(0, ids.size()))
		{
			const auto id = ids[i];
			const auto cell_idx = getCellIdx(id);
			auto & cell = cells[cell_idx];

			for (const auto attribute_idx : ext::range(0, attributes.size()))
			{
				const auto & attribute_column = *column_ptrs[attribute_idx];
				auto & attribute = attributes[attribute_idx];

				setAttributeValue(attribute, cell_idx, attribute_column[i]);
			}

			/// if cell id is zero and zero does not map to this cell, then the cell is unused
			if (cell.id == 0 && cell_idx != zero_cell_idx)
				element_count.fetch_add(1, std::memory_order_relaxed);

			cell.id = id;
			if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
				cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
			else
				cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

			/// inform caller
			on_cell_updated(id, cell_idx);
			/// mark corresponding id as found
			remaining_ids[id] = 1;
		}
	}

	stream->readSuffix();

	/// Check which ids have not been found and require setting null_value
	for (const auto id_found_pair : remaining_ids)
	{
		if (id_found_pair.second)
			continue;

		const auto id = id_found_pair.first;
		const auto cell_idx = getCellIdx(id);
		auto & cell = cells[cell_idx];

		/// Set null_value for each attribute
		for (auto & attribute : attributes)
			setDefaultAttributeValue(attribute, cell_idx);

		/// Check if cell had not been occupied before and increment element counter if it hadn't
		if (cell.id == 0 && cell_idx != zero_cell_idx)
			element_count.fetch_add(1, std::memory_order_relaxed);

		cell.id = id;
		if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
			cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
		else
			cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

		cell.setDefault();

		/// inform caller that the cell has not been found
		on_id_not_found(id, cell_idx);
	}
}


void CacheDictionary::setDefaultAttributeValue(Attribute & attribute, const Key idx) const
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

void CacheDictionary::setAttributeValue(Attribute & attribute, const Key idx, const Field & value) const
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

CacheDictionary::Attribute & CacheDictionary::getAttribute(const std::string & attribute_name) const
{
	const auto it = attribute_index_by_name.find(attribute_name);
	if (it == std::end(attribute_index_by_name))
		throw Exception{
			name + ": no such attribute '" + attribute_name + "'",
			ErrorCodes::BAD_ARGUMENTS
		};

	return attributes[it->second];
}

}
