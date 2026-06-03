# Dictionaries

This directory contains dictionary implementation - a feature that allows loading and caching external data for use in queries. Dictionaries provide a way to enrich queries with data from various external sources like MySQL, PostgreSQL, MongoDB, files etc.

## Overview

The code handles:
- Dictionary interfaces and base classes
- Various dictionary implementations (hashed, cached, flat, etc.)
- Dictionary sources (MySQL, PostgreSQL, HTTP, ClickHouse, etc.)
- Loading and updating dictionary data
- Dictionary configuration parsing

## Key Interfaces & Entry Points

The main interfaces to understand are:

1. `IDictionary.h` - Base interface for all dictionary implementations

    Defines core dictionary functionality like lookups and updates. Key methods: `getColumn()`, `hasKeys()`, etc.

    Example dictionary implementations:
    - `HashedDictionary` - Hash table based dictionary
    - `DirectDictionary` - Dictionary that directly queries source for each lookup
    - `CacheDictionary` - Dictionary which queries source and caches results
    - `FlatDictionary` - Simple array based dictionary for sequential keys
    - `RangeHashedDictionary` - Dictionary optimized for range lookups

1. `IDictionarySource.h` - Interface for dictionary data sources

    Handles loading data from external sources. Key methods: `loadAll()`, `loadIds()`, etc.

    Sources are implemented in files like:

    - `ClickHouseDictionarySource.h`
    - `MySQLDictionarySource.h`
    - `PostgreSQLDictionarySource.h`
    - `HTTPDictionarySource.h`
    - etc.

1. `DictionaryStructure.h` - Describes dictionary schema/configuration

    Attributes, keys, lifetime settings etc.

1. Dictionary implementations and sources are registered using singleton factories (`DictionaryFactory.h/.cpp` and `DictionarySourceFactory.h/.cpp`), with individual factory methods defined in separate `register*.cpp` files containing registration methods for specific dictionary types.

## Implementation Notes

Template specializations are extensively used to implement specialized dictionary classes based on key types (using `DictionaryKeyType` where `Simple` represents `UInt64` keys and `Complex` is used for other or composite keys). Some dictionary implementations also have specializations for particular attribute types, allowing optimized implementations for different data combinations.

## References in other parts of the codebase

Dictionary are reloaded using loaders defined in `Interpreters/ExternalLoader.h` and `/src/Interpreters/ExternalDictionariesLoader.h` registered in the `Context`.

The system tables containing information about active dictionaries are defined in `Storages/System/StorageSystemDictionaries.cpp`.
