#pragma once

namespace DB
{

class DictionarySourceFactory;

void registerDictionarySourceFile(DictionarySourceFactory & source_factory);
void registerDictionarySourceMysql(DictionarySourceFactory & source_factory);
void registerDictionarySourceClickHouse(DictionarySourceFactory & source_factory);
void registerDictionarySourceMongoDB(DictionarySourceFactory & source_factory);
void registerDictionarySourceRedis(DictionarySourceFactory & source_factory);
void registerDictionarySourceXDBC(DictionarySourceFactory & source_factory);
void registerDictionarySourceJDBC(DictionarySourceFactory & source_factory);
void registerDictionarySourceExecutable(DictionarySourceFactory & source_factory);
void registerDictionarySourceHTTP(DictionarySourceFactory & source_factory);
void registerDictionarySourceLibrary(DictionarySourceFactory & source_factory);

class DictionaryFactory;
void registerDictionaryRangeHashed(DictionaryFactory & factory);
void registerDictionaryComplexKeyHashed(DictionaryFactory & factory);
void registerDictionaryComplexKeyCache(DictionaryFactory & factory);
void registerDictionaryTrie(DictionaryFactory & factory);
void registerDictionaryFlat(DictionaryFactory & factory);
void registerDictionaryHashed(DictionaryFactory & factory);
void registerDictionaryCache(DictionaryFactory & factory);
void registerDictionaryPolygon(DictionaryFactory & factory);

void registerDictionaries();
}
