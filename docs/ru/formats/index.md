<a name="formats"></a>

# Форматы входных и выходных данных

ClickHouse может принимать (`INSERT`) и отдавать (`SELECT`) данные в различных форматах.

Поддерживаемые форматы и возможность использовать их в запросах `INSERT` и `SELECT` перечислены в таблице ниже.

Формат | INSERT | SELECT
-------|--------|--------
[TabSeparated](tabseparated.md#tabseparated) | ✔ | ✔ |
[TabSeparatedRaw](tabseparatedraw.md#tabseparatedraw)  | ✗ | ✔ |
[TabSeparatedWithNames](tabseparatedwithnames.md#tabseparatedwithnames) | ✔ | ✔ |
[TabSeparatedWithNamesAndTypes](tabseparatedwithnamesandtypes.md#tabseparatedwithnamesandtypes) | ✔ | ✔ |
[CSV](csv.md#csv) | ✔ | ✔ |
[CSVWithNames](csvwithnames.md#csvwithnames) | ✔ | ✔ |
[Values](values.md#values) | ✔ | ✔ |
[Vertical](vertical.md#vertical) | ✗ | ✔ |
[VerticalRaw](verticalraw.md#verticalraw) | ✗ | ✔ |
[JSON](json.md#json) | ✗ | ✔ |
[JSONCompact](jsoncompact.md#jsoncompact) | ✗ | ✔ |
[JSONEachRow](jsoneachrow.md#jsoneachrow) | ✔ | ✔ |
[TSKV](tskv.md#tskv) | ✔ | ✔ |
[Pretty](pretty.md#pretty) | ✗ | ✔ |
[PrettyCompact](prettycompact.md#prettycompact) | ✗ | ✔ |
[PrettyCompactMonoBlock](prettycompactmonoblock.md#prettycompactmonoblock) | ✗ | ✔ |
[PrettyNoEscapes](prettynoescapes.md#prettynoescapes) | ✗ | ✔ |
[PrettySpace](prettyspace.md#prettyspace) | ✗ | ✔ |
[RowBinary](rowbinary.md#rowbinary) | ✔ | ✔ |
[Native](native.md#native) | ✔ | ✔ |
[Null](null.md#null) | ✗ | ✔ |
[XML](xml.md#xml) | ✗ | ✔ |
[CapnProto](capnproto.md#capnproto) | ✔ | ✔ |
