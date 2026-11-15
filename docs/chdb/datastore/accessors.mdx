---
title: 'DataStore accessors'
sidebarTitle: 'Accessors'
slug: /chdb/datastore/accessors
description: 'String, DateTime, Array, JSON, URL, IP, and Geo accessors with 185+ methods'
keywords: ['chdb', 'datastore', 'accessor', 'str', 'dt', 'arr', 'json', 'url', 'ip', 'geo']
doc_type: 'reference'
---

DataStore provides 7 accessor namespaces with 185+ methods for domain-specific operations.

| Accessor | Methods | Description |
|----------|---------|-------------|
| `.str` | 56 | String operations |
| `.dt` | 42+ | DateTime operations |
| `.arr` | 37 | Array operations (ClickHouse-specific) |
| `.json` | 13 | JSON parsing (ClickHouse-specific) |
| `.url` | 15 | URL parsing (ClickHouse-specific) |
| `.ip` | 9 | IP address operations (ClickHouse-specific) |
| `.geo` | 14 | Geo/distance operations (ClickHouse-specific) |

---

## String Accessor (`.str`) {#str}

All 56 pandas `.str` methods are supported, plus ClickHouse string functions.

### Case Conversion {#str-case}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `upper()` | `upper()` | Convert to uppercase |
| `lower()` | `lower()` | Convert to lowercase |
| `capitalize()` | `initcap()` | Capitalize first letter |
| `title()` | `initcap()` | Title case |
| `swapcase()` | - | Swap case |
| `casefold()` | `lower()` | Case folding |

```python
ds['name_upper'] = ds['name'].str.upper()
ds['name_title'] = ds['name'].str.title()
```

### Length and Size {#str-length}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `len()` | `length()` | String length (bytes) |
| `char_length()` | `char_length()` | Length in characters |

```python
ds['name_len'] = ds['name'].str.len()
```

### Substring and Slicing {#str-substring}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `slice(start, stop)` | `substring()` | Extract substring |
| `slice_replace()` | - | Replace slice |
| `left(n)` | `left()` | Leftmost n characters |
| `right(n)` | `right()` | Rightmost n characters |
| `get(i)` | - | Character at index |

```python
ds['first_3'] = ds['name'].str.slice(0, 3)
ds['last_4'] = ds['name'].str.right(4)
```

### Trimming {#str-trim}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `strip()` | `trim()` | Remove whitespace |
| `lstrip()` | `trimLeft()` | Remove leading whitespace |
| `rstrip()` | `trimRight()` | Remove trailing whitespace |

```python
ds['trimmed'] = ds['text'].str.strip()
```

### Search and Match {#str-search}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `contains(pat)` | `position()` | Contains substring |
| `startswith(pat)` | `startsWith()` | Starts with prefix |
| `endswith(pat)` | `endsWith()` | Ends with suffix |
| `find(sub)` | `position()` | Find position |
| `rfind(sub)` | - | Find from right |
| `index(sub)` | `position()` | Find or raise |
| `rindex(sub)` | - | Find from right or raise |
| `match(pat)` | `match()` | Regex match |
| `fullmatch(pat)` | - | Full regex match |
| `count(pat)` | - | Count occurrences |

```python
# Contains substring
ds['has_john'] = ds['name'].str.contains('John')

# Regex match
ds['valid_email'] = ds['email'].str.match(r'^[\w.-]+@[\w.-]+\.\w+$')
```

### Replace {#str-replace}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `replace(pat, repl)` | `replace()` | Replace occurrences |
| `replace(pat, repl, regex=True)` | `replaceRegexpAll()` | Regex replace |
| `removeprefix(prefix)` | - | Remove prefix |
| `removesuffix(suffix)` | - | Remove suffix |
| `translate(table)` | - | Translate characters |

```python
ds['cleaned'] = ds['text'].str.replace('\n', ' ')
ds['digits_only'] = ds['phone'].str.replace(r'\D', '', regex=True)
```

### Splitting {#str-split}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `split(sep)` | `splitByString()` | Split into array |
| `rsplit(sep)` | - | Split from right |
| `partition(sep)` | - | Split into 3 parts |
| `rpartition(sep)` | - | Split from right into 3 |

```python
ds['parts'] = ds['path'].str.split('/')
```

### Padding {#str-padding}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `pad(width)` | `leftPad()` | Left pad |
| `ljust(width)` | `rightPad()` | Right justify |
| `rjust(width)` | `leftPad()` | Left justify |
| `center(width)` | - | Center |
| `zfill(width)` | `leftPad(..., '0')` | Zero fill |

```python
ds['padded_id'] = ds['id'].astype(str).str.zfill(6)
```

### Character Tests {#str-tests}

| Method | Description |
|--------|-------------|
| `isalpha()` | All alphabetic |
| `isdigit()` | All digits |
| `isalnum()` | Alphanumeric |
| `isspace()` | All whitespace |
| `isupper()` | All uppercase |
| `islower()` | All lowercase |
| `istitle()` | Title case |
| `isnumeric()` | Numeric characters |
| `isdecimal()` | Decimal characters |

```python
ds['is_numeric'] = ds['code'].str.isdigit()
```

### Other {#str-other}

| Method | Description |
|--------|-------------|
| `repeat(n)` | Repeat n times |
| `reverse()` | Reverse string |
| `wrap(width)` | Wrap text |
| `encode(enc)` | Encode |
| `decode(enc)` | Decode |
| `normalize(form)` | Unicode normalize |
| `extract(pat)` | Extract regex groups |
| `extractall(pat)` | Extract all matches |
| `cat(sep)` | Concatenate all |
| `get_dummies(sep)` | Dummy variables |

---

## DateTime Accessor (`.dt`) {#dt}

All 42+ pandas `.dt` methods plus ClickHouse datetime functions.

### Date Components {#dt-components}

| Property | ClickHouse | Description |
|----------|------------|-------------|
| `year` | `toYear()` | Year |
| `month` | `toMonth()` | Month (1-12) |
| `day` | `toDayOfMonth()` | Day (1-31) |
| `hour` | `toHour()` | Hour (0-23) |
| `minute` | `toMinute()` | Minute (0-59) |
| `second` | `toSecond()` | Second (0-59) |
| `millisecond` | `toMillisecond()` | Millisecond |
| `microsecond` | `toMicrosecond()` | Microsecond |
| `quarter` | `toQuarter()` | Quarter (1-4) |
| `dayofweek` | `toDayOfWeek()` | Day of week (0=Mon) |
| `dayofyear` | `toDayOfYear()` | Day of year |
| `week` | `toWeek()` | Week number |
| `days_in_month` | - | Days in month |

```python
ds['year'] = ds['date'].dt.year
ds['month'] = ds['date'].dt.month
ds['day_of_week'] = ds['date'].dt.dayofweek
```

### Truncation {#dt-truncation}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `to_start_of_day()` | `toStartOfDay()` | Start of day |
| `to_start_of_week()` | `toStartOfWeek()` | Start of week |
| `to_start_of_month()` | `toStartOfMonth()` | Start of month |
| `to_start_of_quarter()` | `toStartOfQuarter()` | Start of quarter |
| `to_start_of_year()` | `toStartOfYear()` | Start of year |
| `to_start_of_hour()` | `toStartOfHour()` | Start of hour |
| `to_start_of_minute()` | `toStartOfMinute()` | Start of minute |

```python
ds['month_start'] = ds['date'].dt.to_start_of_month()
```

### Arithmetic {#dt-arithmetic}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `add_years(n)` | `addYears()` | Add years |
| `add_months(n)` | `addMonths()` | Add months |
| `add_weeks(n)` | `addWeeks()` | Add weeks |
| `add_days(n)` | `addDays()` | Add days |
| `add_hours(n)` | `addHours()` | Add hours |
| `add_minutes(n)` | `addMinutes()` | Add minutes |
| `add_seconds(n)` | `addSeconds()` | Add seconds |
| `subtract_years(n)` | `subtractYears()` | Subtract years |
| `subtract_months(n)` | `subtractMonths()` | Subtract months |
| `subtract_days(n)` | `subtractDays()` | Subtract days |

```python
ds['next_month'] = ds['date'].dt.add_months(1)
ds['last_week'] = ds['date'].dt.subtract_weeks(1)
```

### Boolean Checks {#dt-checks}

| Method | Description |
|--------|-------------|
| `is_month_start()` | First day of month |
| `is_month_end()` | Last day of month |
| `is_quarter_start()` | First day of quarter |
| `is_quarter_end()` | Last day of quarter |
| `is_year_start()` | First day of year |
| `is_year_end()` | Last day of year |
| `is_leap_year()` | Leap year |

```python
ds['is_eom'] = ds['date'].dt.is_month_end()
```

### Formatting {#dt-formatting}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `strftime(fmt)` | `formatDateTime()` | Format as string |
| `day_name()` | - | Day name |
| `month_name()` | - | Month name |

```python
ds['date_str'] = ds['date'].dt.strftime('%Y-%m-%d')
ds['day_name'] = ds['date'].dt.day_name()
```

### Timezone {#dt-timezone}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `tz_convert(tz)` | `toTimezone()` | Convert timezone |
| `tz_localize(tz)` | - | Localize timezone |

```python
ds['utc_time'] = ds['timestamp'].dt.tz_convert('UTC')
```

---

## Array Accessor (`.arr`) {#arr}

ClickHouse-specific array operations (37 methods).

### Properties {#arr-properties}

| Property | ClickHouse | Description |
|----------|------------|-------------|
| `length` | `length()` | Array length |
| `size` | `length()` | Alias for length |
| `empty` | `empty()` | Is empty |
| `not_empty` | `notEmpty()` | Is not empty |

```python
ds['tag_count'] = ds['tags'].arr.length
ds['has_tags'] = ds['tags'].arr.not_empty
```

### Element Access {#arr-access}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `array_first()` | `arrayElement(..., 1)` | First element |
| `array_last()` | `arrayElement(..., -1)` | Last element |
| `array_element(n)` | `arrayElement()` | Nth element |
| `array_slice(off, len)` | `arraySlice()` | Slice array |

```python
ds['first_tag'] = ds['tags'].arr.array_first()
ds['last_tag'] = ds['tags'].arr.array_last()
```

### Aggregations {#arr-aggregations}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `array_sum()` | `arraySum()` | Sum of elements |
| `array_avg()` | `arrayAvg()` | Average |
| `array_min()` | `arrayMin()` | Minimum |
| `array_max()` | `arrayMax()` | Maximum |
| `array_product()` | `arrayProduct()` | Product |
| `array_uniq()` | `arrayUniq()` | Count unique |

```python
ds['total'] = ds['values'].arr.array_sum()
ds['average'] = ds['values'].arr.array_avg()
```

### Transformations {#arr-transformations}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `array_sort()` | `arraySort()` | Sort ascending |
| `array_reverse_sort()` | `arrayReverseSort()` | Sort descending |
| `array_reverse()` | `arrayReverse()` | Reverse order |
| `array_distinct()` | `arrayDistinct()` | Unique elements |
| `array_compact()` | `arrayCompact()` | Remove consecutive dupes |
| `array_flatten()` | `arrayFlatten()` | Flatten nested |

```python
ds['sorted_tags'] = ds['tags'].arr.array_sort()
ds['unique_tags'] = ds['tags'].arr.array_distinct()
```

### Modifications {#arr-modifications}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `array_push_back(elem)` | `arrayPushBack()` | Add to end |
| `array_push_front(elem)` | `arrayPushFront()` | Add to front |
| `array_pop_back()` | `arrayPopBack()` | Remove last |
| `array_pop_front()` | `arrayPopFront()` | Remove first |
| `array_concat(other)` | `arrayConcat()` | Concatenate |

### Search {#arr-search}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `has(elem)` | `has()` | Contains element |
| `index_of(elem)` | `indexOf()` | Find index |
| `count_equal(elem)` | `countEqual()` | Count occurrences |

```python
ds['has_python'] = ds['skills'].arr.has('Python')
```

### String Operations {#arr-string}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `array_string_concat(sep)` | `arrayStringConcat()` | Join to string |

```python
ds['tags_str'] = ds['tags'].arr.array_string_concat(', ')
```

---

## JSON Accessor (`.json`) {#json}

ClickHouse-specific JSON parsing (13 methods).

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `get_string(path)` | `JSONExtractString()` | Extract string |
| `get_int(path)` | `JSONExtractInt()` | Extract integer |
| `get_float(path)` | `JSONExtractFloat()` | Extract float |
| `get_bool(path)` | `JSONExtractBool()` | Extract boolean |
| `get_raw(path)` | `JSONExtractRaw()` | Extract raw JSON |
| `get_keys()` | `JSONExtractKeys()` | Get keys |
| `get_type(path)` | `JSONType()` | Get type |
| `get_length(path)` | `JSONLength()` | Get length |
| `has_key(key)` | `JSONHas()` | Check key exists |
| `is_valid()` | `isValidJSON()` | Validate JSON |
| `to_json_string()` | `toJSONString()` | Convert to JSON |

```python
# Parse JSON columns
ds['user_name'] = ds['json_data'].json.get_string('user.name')
ds['user_age'] = ds['json_data'].json.get_int('user.age')
ds['is_active'] = ds['json_data'].json.get_bool('user.active')
ds['has_email'] = ds['json_data'].json.has_key('user.email')
```

---

## URL Accessor (`.url`) {#url}

ClickHouse-specific URL parsing (15 methods).

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `domain()` | `domain()` | Extract domain |
| `domain_without_www()` | `domainWithoutWWW()` | Domain without www |
| `top_level_domain()` | `topLevelDomain()` | TLD |
| `protocol()` | `protocol()` | Protocol (http/https) |
| `path()` | `path()` | URL path |
| `path_full()` | `pathFull()` | Path with query |
| `query_string()` | `queryString()` | Query string |
| `fragment()` | `fragment()` | Fragment (#...) |
| `port()` | `port()` | Port number |
| `extract_url_parameter(name)` | `extractURLParameter()` | Get query param |
| `extract_url_parameters()` | `extractURLParameters()` | All params |
| `cut_url_parameter(name)` | `cutURLParameter()` | Remove param |
| `decode_url_component()` | `decodeURLComponent()` | URL decode |
| `encode_url_component()` | `encodeURLComponent()` | URL encode |

```python
# Parse URLs
ds['domain'] = ds['url'].url.domain()
ds['path'] = ds['url'].url.path()
ds['utm_source'] = ds['url'].url.extract_url_parameter('utm_source')
```

---

## IP Accessor (`.ip`) {#ip}

ClickHouse-specific IP address operations (9 methods).

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `to_ipv4()` | `toIPv4()` | Convert to IPv4 |
| `to_ipv6()` | `toIPv6()` | Convert to IPv6 |
| `ipv4_num_to_string()` | `IPv4NumToString()` | Number to string |
| `ipv4_string_to_num()` | `IPv4StringToNum()` | String to number |
| `ipv6_num_to_string()` | `IPv6NumToString()` | IPv6 num to string |
| `ipv4_to_ipv6()` | `IPv4ToIPv6()` | Convert to IPv6 |
| `is_ipv4_string()` | `isIPv4String()` | Validate IPv4 |
| `is_ipv6_string()` | `isIPv6String()` | Validate IPv6 |
| `ipv4_cidr_to_range(cidr)` | `IPv4CIDRToRange()` | CIDR to range |

```python
# IP operations
ds['is_valid_ip'] = ds['ip'].ip.is_ipv4_string()
ds['ip_num'] = ds['ip'].ip.ipv4_string_to_num()
```

---

## Geo Accessor (`.geo`) {#geo}

ClickHouse-specific geo/distance operations (14 methods).

### Distance Functions {#geo-distance}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `great_circle_distance(...)` | `greatCircleDistance()` | Great circle distance |
| `geo_distance(...)` | `geoDistance()` | WGS-84 distance |
| `l1_distance(v1, v2)` | `L1Distance()` | Manhattan distance |
| `l2_distance(v1, v2)` | `L2Distance()` | Euclidean distance |
| `l2_squared_distance(v1, v2)` | `L2SquaredDistance()` | Squared Euclidean |
| `linf_distance(v1, v2)` | `LinfDistance()` | Chebyshev distance |
| `cosine_distance(v1, v2)` | `cosineDistance()` | Cosine distance |

### Vector Operations {#geo-vector}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `dot_product(v1, v2)` | `dotProduct()` | Dot product |
| `l2_norm(vec)` | `L2Norm()` | Vector norm |
| `l2_normalize(vec)` | `L2Normalize()` | Normalize |

### H3 Functions {#geo-h3}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `geo_to_h3(lon, lat, res)` | `geoToH3()` | Geo to H3 index |
| `h3_to_geo(h3)` | `h3ToGeo()` | H3 to geo coords |

### Point Operations {#geo-point}

| Method | ClickHouse | Description |
|--------|------------|-------------|
| `point_in_polygon(pt, poly)` | `pointInPolygon()` | Point in polygon |
| `point_in_ellipses(...)` | `pointInEllipses()` | Point in ellipses |

```python
from chdb.datastore import F

# Calculate distances
ds['distance'] = F.great_circle_distance(
    ds['lon1'], ds['lat1'],
    ds['lon2'], ds['lat2']
)

# Vector similarity
ds['similarity'] = F.cosine_distance(ds['embedding1'], ds['embedding2'])
```

---

## Using Accessors {#using-accessors}

### Lazy Evaluation {#lazy}

Most accessor methods are lazy - they return expressions that are evaluated later:

```python
# All these are lazy
ds['name_upper'] = ds['name'].str.upper()  # Not executed yet
ds['year'] = ds['date'].dt.year            # Not executed yet
ds['domain'] = ds['url'].url.domain()      # Not executed yet

# Execution happens when you access results
df = ds.to_df()  # Now everything executes
```

### Methods That Execute Immediately {#execute-immediately}

Some `.str` methods must execute because they change the structure:

| Method | Returns | Why |
|--------|---------|-----|
| `partition(sep)` | DataStore (3 columns) | Creates multiple columns |
| `rpartition(sep)` | DataStore (3 columns) | Creates multiple columns |
| `get_dummies(sep)` | DataStore (N columns) | Dynamic column count |
| `extractall(pat)` | DataStore | MultiIndex result |
| `cat(sep)` | str | Aggregation (N rows → 1) |

### Chaining Accessors {#chaining}

Accessor methods can be chained:

```python
ds['clean_name'] = (ds['name']
    .str.strip()
    .str.lower()
    .str.replace(' ', '_')
)

ds['next_month_start'] = (ds['date']
    .dt.add_months(1)
    .dt.to_start_of_month()
)
```
