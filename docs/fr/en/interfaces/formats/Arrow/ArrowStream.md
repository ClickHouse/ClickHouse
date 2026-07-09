---
alias: []
description: 'Documentation sur le format ArrowStream'
input_format: true
keywords: ['ArrowStream']
output_format: true
slug: /interfaces/formats/ArrowStream
title: 'ArrowStream'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

`ArrowStream` est le format « en mode flux » d’Apache Arrow. Il est conçu pour le traitement en mémoire des flux.

<div id="example-usage">
  ## Exemple d&#39;utilisation
</div>

Dans l&#39;exemple ci-dessous, nous utilisons le dataset `forex`, disponible dans le
[ClickHouse SQL playground](https://sql.clickhouse.com). Vous pouvez vous y connecter
à distance avec `clickhouse-client` en utilisant l’hôte `sql-clickhouse.clickhouse.com`
et l’utilisateur `demo` (qui n’a pas de mot de passe). La table `forex` se trouve dans la
base de données `forex`, nous la sélectionnons donc comme base de données par défaut :

```bash
clickhouse-client --secure --host sql-clickhouse.clickhouse.com --user demo --database forex
```

La table `forex` stocke les taux de change des devises. Nous pouvons examiner sa taille et
son taux de compression sur disque en interrogeant [`system.columns`](/fr/operations/system-tables/columns) :

```sql title="Query"
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) AS compression_ratio
FROM system.columns
WHERE (database = 'forex') AND (table = 'forex')
GROUP BY table
ORDER BY table ASC
```

```response title="Response"
   ┌─table─┬─compressed_size─┬─uncompressed_size─┬───compression_ratio─┐
1. │ forex │ 63.69 GiB       │ 280.48 GiB        │ 0.22708227109363446 │
   └───────┴─────────────────┴───────────────────┴─────────────────────┘
```

Contrairement au format [`Arrow`](/fr/interfaces/formats/Arrow) « mode fichier », qui
nécessite que le résultat complet soit disponible avant de pouvoir être lu, `ArrowStream` est fourni sous la
forme d&#39;une séquence de lots d’enregistrements qu&#39;un consumer peut lire de manière incrémentale à mesure
de leur arrivée. Il est donc particulièrement bien adapté au streaming d&#39;un résultat de la requête directement vers un
outil de visualisation ou d&#39;analyse, sans devoir d&#39;abord matérialiser l&#39;intégralité du dataset.

Pour diffuser le résultat en flux, envoyez la query via l&#39;HTTP interface de ClickHouse avec une
requête `POST`, puis lisez la réponse comme un flux Arrow. Nous désactivons la Compression
de la sortie Arrow via le paramètre
[`output_format_arrow_compression_method`](/fr/operations/settings/formats#output_format_arrow_compression_method)
afin que les consumers puissent décoder les lots d’enregistrements directement à mesure qu&#39;ils sont reçus.

La sortie `ArrowStream` est du binaire brut ; au lieu de l&#39;afficher dans le
terminal, nous la redirigeons donc vers un consumer. Le flux est auto-descriptif (il transporte
son propre schéma) ; nous le redirigeons donc ici directement vers
[`clickhouse-local`](/fr/operations/utilities/clickhouse-local), qui lit les
lots d’enregistrements entrants avec `--input-format ArrowStream` et les interroge comme une table.
La table `forex` est volumineuse ; nous limitons donc la remote query avec un
predicate `WHERE` et un `LIMIT` afin de garder cet exemple concis :

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'USD' AND quote = 'CHF'
        ORDER BY datetime ASC
        LIMIT 5
        FORMAT ArrowStream
        SETTINGS output_format_arrow_compression_method='none'" \
  | clickhouse-local --input-format ArrowStream \
      --query "SELECT * FROM table ORDER BY last_update ASC FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬────bid─┬────ask─┬────────────────spread─┐
1. │ USD.CHF    │ 2000-05-30 17:23:44.000 │  1.688 │ 1.6885 │ 0.0005000829696655273 │
2. │ USD.CHF    │ 2000-05-30 17:23:46.000 │ 1.6885 │  1.689 │ 0.0004999637603759766 │
3. │ USD.CHF    │ 2000-05-30 17:23:48.000 │ 1.6886 │ 1.6891 │ 0.0005000829696655273 │
4. │ USD.CHF    │ 2000-05-30 17:23:49.000 │ 1.6888 │ 1.6893 │ 0.0004999637603759766 │
5. │ USD.CHF    │ 2000-05-30 17:24:45.000 │  1.689 │ 1.6895 │ 0.0004999637603759766 │
   └────────────┴─────────────────────────┴────────┴────────┴───────────────────────┘
```

Le même flux peut être consommé de manière incrémentale par tout client compatible
avec Arrow, qui le lit lot par lot au lieu de mettre en mémoire tampon l’intégralité du résultat. Par exemple,
en utilisant la [bibliothèque JavaScript Apache Arrow](https://arrow.apache.org/docs/js/), un
`RecordBatchReader` produit chaque lot d’enregistrements dès qu’il est transmis par le
serveur :

```js
const reader = await RecordBatchReader.from(response);
await reader.open();
for await (const recordBatch of reader) {
    const batchTable = new Table(recordBatch);
    const ipcStream = tableToIPC(batchTable, 'stream');
    const bytes = new Uint8Array(ipcStream);
    table.update(bytes);
}
```

Pour un guide complet sur le streaming de données `ArrowStream` de ClickHouse dans une
visualisation en temps réel avec [Perspective](https://perspective.finos.org/), consultez
l’article de blog
[Streaming real-time visualizations with ClickHouse, Apache Arrow and Perspective](https://clickhouse.com/blog/streaming-real-time-visualizations-clickhouse-apache-arrow-perpsective).

<div id="format-settings">
  ## Paramètres du format
</div>

`ArrowStream` utilise les mêmes paramètres de format que le format [`Arrow`](/fr/interfaces/formats/Arrow).

| Paramètre                                                                    | Description                                                                                                                                               | Par défaut  |
| ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | Autoriser les colonnes manquantes lors de la lecture des formats d&#39;entrée Arrow                                                                       | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Ignorer la casse lors de la correspondance entre les colonnes Arrow et les colonnes CH.                                                                   | `0`         |
| `input_format_arrow_import_nested`                                           | Paramètre obsolète, sans effet.                                                                                                                           | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Ignorer les colonnes dont les types ne sont pas pris en charge lors de l&#39;inférence de schéma pour le format Arrow                                     | `0`         |
| `output_format_arrow_compression_method`                                     | Méthode de compression du format de sortie Arrow. Codecs pris en charge : lz4&#95;frame, zstd, none (non compressé)                                       | `lz4_frame` |
| `output_format_arrow_date_as_uint16`                                         | Écrire les valeurs Date comme de simples nombres 16 bits (relus comme UInt16), au lieu de les convertir en type Arrow DATE32 32 bits (relu comme Date32). | `0`         |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Utiliser le type Arrow FIXED&#95;SIZE&#95;BINARY au lieu de Binary pour les colonnes FixedString.                                                         | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Activer la sortie du type LowCardinality en tant que type Arrow Dictionary                                                                                | `0`         |
| `output_format_arrow_string_as_string`                                       | Utiliser le type String d&#39;Arrow au lieu de Binary pour les colonnes de type String                                                                    | `1`         |
| `output_format_arrow_unsupported_types_as_binary`                            | Exporter les types sans conversion possible sous forme de données binaires brutes. Si false, ces types déclenchent une exception UNKNOWN&#95;TYPE.        | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Toujours utiliser des entiers 64 bits pour les index de dictionnaire au format Arrow                                                                      | `0`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Utiliser des entiers signés pour les index de dictionnaire au format Arrow                                                                                | `1`         |