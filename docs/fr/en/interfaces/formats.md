---
description: 'Aperçu des formats de données pris en charge en entrée et en sortie dans ClickHouse'
sidebar_label: 'Voir tous les formats...'
sidebar_position: 21
slug: /interfaces/formats
title: 'Formats de données d’entrée et de sortie'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="formats-for-input-and-output-data">
  # Formats de données d’entrée et de sortie
</div>

ClickHouse prend en charge la plupart des formats de texte et de données binaires courants. Cela facilite son intégration dans presque n’importe quel pipeline de données afin de tirer parti des avantages de ClickHouse.

<div id="input-formats">
  ## Formats d’entrée
</div>

Les formats d’entrée sont utilisés pour :

* analyser les données fournies aux instructions `INSERT`
* exécuter des requêtes `SELECT` sur des tables basées sur des fichiers, comme `File`, `URL` ou `HDFS`
* lire des dictionnaires

Choisir le bon format d’entrée est essentiel pour une ingestion de données efficace dans ClickHouse. Avec plus de 70 formats pris en charge,
le choix de l’option la plus performante peut avoir un impact significatif sur la vitesse d’insertion, l’utilisation du CPU et de la mémoire, ainsi que sur l’efficacité globale
du système. Pour vous aider à vous y retrouver, nous avons comparé les performances d’ingestion selon les formats, ce qui a fait ressortir les principaux points suivants :

* **Le format [Native](formats/Native.md) est le format d’entrée le plus efficace**, offrant la meilleure compression, la plus faible
  utilisation des ressources et une surcharge minimale de traitement côté serveur.
* **La compression est essentielle** - LZ4 réduit la taille des données avec un coût CPU minimal, tandis que ZSTD offre une compression plus élevée au
  prix d’une utilisation supplémentaire du CPU.
* **Le pré-tri a un impact modéré**, car ClickHouse trie déjà efficacement.
* **Le traitement par lots améliore considérablement l’efficacité** - des lots plus volumineux réduisent la surcharge d’insertion et améliorent le débit.

Pour une analyse approfondie des résultats et des bonnes pratiques,
consultez l’[analyse complète du benchmark](https://www.clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient).
Pour consulter l’ensemble des résultats des tests, explorez le [dashboard](https://fastformats.clickhouse.com/) FastFormats en ligne.

<div id="output-formats">
  ## Formats de sortie
</div>

Les formats de sortie pris en charge sont utilisés pour :

* Organiser les résultats d’une requête `SELECT`
* Effectuer des opérations `INSERT` dans des tables reposant sur des fichiers

<div id="formats-overview">
  ## Vue d’ensemble des formats
</div>

Les formats pris en charge sont les suivants :

| Format                                                                                                     | Entrée | Sortie |
| ---------------------------------------------------------------------------------------------------------- | ------ | ------ |
| [TabSeparated](./formats/TabSeparated/TabSeparated.md)                                                     | ✔      | ✔      |
| [TabSeparatedRaw](./formats/TabSeparated/TabSeparatedRaw.md)                                               | ✔      | ✔      |
| [TabSeparatedWithNames](./formats/TabSeparated/TabSeparatedWithNames.md)                                   | ✔      | ✔      |
| [TabSeparatedWithNamesAndTypes](./formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)                   | ✔      | ✔      |
| [TabSeparatedRawWithNames](./formats/TabSeparated/TabSeparatedRawWithNames.md)                             | ✔      | ✔      |
| [TabSeparatedRawWithNamesAndTypes](./formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)             | ✔      | ✔      |
| [Template](./formats/Template/Template.md)                                                                 | ✔      | ✔      |
| [TemplateIgnoreSpaces](./formats/Template/TemplateIgnoreSpaces.md)                                         | ✔      | ✗      |
| [CSV](./formats/CSV/CSV.md)                                                                                | ✔      | ✔      |
| [CSVWithNames](./formats/CSV/CSVWithNames.md)                                                              | ✔      | ✔      |
| [CSVWithNamesAndTypes](./formats/CSV/CSVWithNamesAndTypes.md)                                              | ✔      | ✔      |
| [CustomSeparated](./formats/CustomSeparated/CustomSeparated.md)                                            | ✔      | ✔      |
| [CustomSeparatedWithNames](./formats/CustomSeparated/CustomSeparatedWithNames.md)                          | ✔      | ✔      |
| [CustomSeparatedWithNamesAndTypes](./formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)          | ✔      | ✔      |
| [SQLInsert](./formats/SQLInsert.md)                                                                        | ✗      | ✔      |
| [Values](./formats/Values.md)                                                                              | ✔      | ✔      |
| [Vertical](./formats/Vertical.md)                                                                          | ✗      | ✔      |
| [JSON](./formats/JSON/JSON.md)                                                                             | ✔      | ✔      |
| [JSONAsString](./formats/JSON/JSONAsString.md)                                                             | ✔      | ✗      |
| [JSONAsObject](./formats/JSON/JSONAsObject.md)                                                             | ✔      | ✗      |
| [JSONStrings](./formats/JSON/JSONStrings.md)                                                               | ✗      | ✔      |
| [JSONColumns](./formats/JSON/JSONColumns.md)                                                               | ✔      | ✔      |
| [JSONColumnsWithMetadata](./formats/JSON/JSONColumnsWithMetadata.md)                                       | ✔      | ✔      |
| [JSONCompact](./formats/JSON/JSONCompact.md)                                                               | ✔      | ✔      |
| [JSONCompactStrings](./formats/JSON/JSONCompactStrings.md)                                                 | ✗      | ✔      |
| [JSONCompactColumns](./formats/JSON/JSONCompactColumns.md)                                                 | ✔      | ✔      |
| [JSONEachRow](./formats/JSON/JSONEachRow.md)                                                               | ✔      | ✔      |
| [PrettyJSONEachRow](./formats/JSON/PrettyJSONEachRow.md)                                                   | ✗      | ✔      |
| [JSONEachRowWithProgress](./formats/JSON/JSONEachRowWithProgress.md)                                       | ✗      | ✔      |
| [JSONStringsEachRow](./formats/JSON/JSONStringsEachRow.md)                                                 | ✔      | ✔      |
| [JSONStringsEachRowWithProgress](./formats/JSON/JSONStringsEachRowWithProgress.md)                         | ✗      | ✔      |
| [JSONCompactEachRow](./formats/JSON/JSONCompactEachRow.md)                                                 | ✔      | ✔      |
| [JSONCompactEachRowWithNames](./formats/JSON/JSONCompactEachRowWithNames.md)                               | ✔      | ✔      |
| [JSONCompactEachRowWithNamesAndTypes](./formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)               | ✔      | ✔      |
| [JSONCompactEachRowWithProgress](./formats/JSON/JSONCompactEachRowWithProgress.md)                         | ✗      | ✔      |
| [JSONCompactStringsEachRow](./formats/JSON/JSONCompactStringsEachRow.md)                                   | ✔      | ✔      |
| [JSONCompactStringsEachRowWithNames](./formats/JSON/JSONCompactStringsEachRowWithNames.md)                 | ✔      | ✔      |
| [JSONCompactStringsEachRowWithNamesAndTypes](./formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md) | ✔      | ✔      |
| [JSONCompactStringsEachRowWithProgress](./formats/JSON/JSONCompactStringsEachRowWithProgress.md)           | ✗      | ✔      |
| [JSONObjectEachRow](./formats/JSON/JSONObjectEachRow.md)                                                   | ✔      | ✔      |
| [BSONEachRow](./formats/BSONEachRow.md)                                                                    | ✔      | ✔      |
| [TSKV](./formats/TabSeparated/TSKV.md)                                                                     | ✔      | ✔      |
| [Pretty](./formats/Pretty/Pretty.md)                                                                       | ✗      | ✔      |
| [PrettyNoEscapes](./formats/Pretty/PrettyNoEscapes.md)                                                     | ✗      | ✔      |
| [PrettyMonoBlock](./formats/Pretty/PrettyMonoBlock.md)                                                     | ✗      | ✔      |
| [PrettyNoEscapesMonoBlock](./formats/Pretty/PrettyNoEscapesMonoBlock.md)                                   | ✗      | ✔      |
| [PrettyCompact](./formats/Pretty/PrettyCompact.md)                                                         | ✗      | ✔      |
| [PrettyCompactNoEscapes](./formats/Pretty/PrettyCompactNoEscapes.md)                                       | ✗      | ✔      |
| [PrettyCompactMonoBlock](./formats/Pretty/PrettyCompactMonoBlock.md)                                       | ✗      | ✔      |
| [PrettyCompactNoEscapesMonoBlock](./formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)                     | ✗      | ✔      |
| [PrettySpace](./formats/Pretty/PrettySpace.md)                                                             | ✗      | ✔      |
| [PrettySpaceNoEscapes](./formats/Pretty/PrettySpaceNoEscapes.md)                                           | ✗      | ✔      |
| [PrettySpaceMonoBlock](./formats/Pretty/PrettySpaceMonoBlock.md)                                           | ✗      | ✔      |
| [PrettySpaceNoEscapesMonoBlock](./formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)                         | ✗      | ✔      |
| [Prometheus](./formats/Prometheus.md)                                                                      | ✗      | ✔      |
| [Protobuf](./formats/Protobuf/Protobuf.md)                                                                 | ✔      | ✔      |
| [ProtobufSingle](./formats/Protobuf/ProtobufSingle.md)                                                     | ✔      | ✔      |
| [ProtobufList](./formats/Protobuf/ProtobufList.md)                                                         | ✔      | ✔      |
| [Avro](./formats/Avro/Avro.md)                                                                             | ✔      | ✔      |
| [AvroConfluent](./formats/Avro/AvroConfluent.md)                                                           | ✔      | ✔      |
| [Parquet](./formats/Parquet/Parquet.md)                                                                    | ✔      | ✔      |
| [ParquetMetadata](./formats/Parquet/ParquetMetadata.md)                                                    | ✔      | ✗      |
| [Arrow](./formats/Arrow/Arrow.md)                                                                          | ✔      | ✔      |
| [ArrowStream](./formats/Arrow/ArrowStream.md)                                                              | ✔      | ✔      |
| [ORC](./formats/ORC.md)                                                                                    | ✔      | ✔      |
| [One](./formats/One.md)                                                                                    | ✔      | ✗      |
| [Npy](./formats/Npy.md)                                                                                    | ✔      | ✔      |
| [RowBinary](./formats/RowBinary/RowBinary.md)                                                              | ✔      | ✔      |
| [RowBinaryWithNames](./formats/RowBinary/RowBinaryWithNames.md)                                            | ✔      | ✔      |
| [RowBinaryWithNamesAndTypes](./formats/RowBinary/RowBinaryWithNamesAndTypes.md)                            | ✔      | ✔      |
| [RowBinaryWithDefaults](./formats/RowBinary/RowBinaryWithDefaults.md)                                      | ✔      | ✗      |
| [RowBinaryWithNamesAndTypesAndDefaults](./formats/RowBinary/RowBinaryWithNamesAndTypesAndDefaults.md)      | ✔      | ✗      |
| [Native](./formats/Native.md)                                                                              | ✔      | ✔      |
| [Buffers](./formats/Buffers.md)                                                                            | ✔      | ✔      |
| [Null](./formats/Null.md)                                                                                  | ✗      | ✔      |
| [Hash](./formats/Hash.md)                                                                                  | ✗      | ✔      |
| [XML](./formats/XML.md)                                                                                    | ✗      | ✔      |
| [CapnProto](./formats/CapnProto.md)                                                                        | ✔      | ✔      |
| [LineAsString](./formats/LineAsString/LineAsString.md)                                                     | ✔      | ✔      |
| [LineAsStringWithNames](./formats/LineAsString/LineAsStringWithNames.md)                                   | ✗      | ✔      |
| [LineAsStringWithNamesAndTypes](./formats/LineAsString/LineAsStringWithNamesAndTypes.md)                   | ✗      | ✔      |
| [Regexp](./formats/Regexp.md)                                                                              | ✔      | ✗      |
| [RawBLOB](./formats/RawBLOB.md)                                                                            | ✔      | ✔      |
| [MsgPack](./formats/MsgPack.md)                                                                            | ✔      | ✔      |
| [MySQLDump](./formats/MySQLDump.md)                                                                        | ✔      | ✗      |
| [GeoJSON](./formats/GeoJSON.md)                                                                            | ✔      | ✔      |
| [DWARF](./formats/DWARF.md)                                                                                | ✔      | ✗      |
| [Markdown](./formats/Markdown.md)                                                                          | ✗      | ✔      |
| [Form](./formats/Form.md)                                                                                  | ✔      | ✗      |

Vous pouvez contrôler certains paramètres de traitement des formats à l’aide des paramètres de ClickHouse. Pour en savoir plus, consultez la section [Paramètres](/fr/operations/settings/settings-formats.md).

<div id="formatschema">
  ## Schéma du format
</div>

Le nom du fichier contenant le schéma du format est défini par le paramètre `format_schema`.
Il est nécessaire de définir ce paramètre lors de l&#39;utilisation de l&#39;un des formats `Cap'n Proto` ou `Protobuf`.
Le schéma du format combine un nom de fichier et le nom d&#39;un type de message dans ce fichier, séparés par deux-points,
par ex. `schemafile.proto:MessageType`.
Si le fichier a l&#39;extension standard du format (par exemple, `.proto` pour `Protobuf`),
celle-ci peut être omise et, dans ce cas, le schéma du format se présente sous la forme `schemafile:MessageType`.

Si vous importez ou exportez des données via le [client](/fr/interfaces/client.md) en mode interactif, le nom de fichier spécifié dans le schéma du format
peut contenir un chemin absolu ou un chemin relatif au répertoire courant sur le client.
Si vous utilisez le client en [mode batch](/fr/interfaces/client.md/#batch-mode), le chemin vers le schéma doit être relatif pour des raisons de sécurité.

Si vous importez ou exportez des données via l&#39;[interface HTTP](/fr/interfaces/http), le nom de fichier spécifié dans le schéma du format
doit se trouver dans le répertoire spécifié par [format&#95;schema&#95;path](/fr/operations/server-configuration-parameters/settings.md/#format_schema_path)
dans la configuration du serveur.

<div id="skippingerrors">
  ## Ignorer les erreurs
</div>

Certains formats, tels que `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` et `Protobuf`, peuvent ignorer une ligne invalide en cas d&#39;erreur d&#39;analyse et reprendre l&#39;analyse au début de la ligne suivante. Voir les paramètres [input&#95;format&#95;allow&#95;errors&#95;num](/fr/operations/settings/settings-formats.md/#input_format_allow_errors_num) et
[input&#95;format&#95;allow&#95;errors&#95;ratio](/fr/operations/settings/settings-formats.md/#input_format_allow_errors_ratio).
Limitations :

* En cas d&#39;erreur d&#39;analyse, `JSONEachRow` ignore toutes les données jusqu&#39;au saut de ligne suivant (ou à EOF). Les lignes doivent donc être délimitées par `\n` pour que les erreurs soient correctement comptabilisées.
* `Template` et `CustomSeparated` utilisent le délimiteur après la dernière colonne ainsi que le délimiteur entre les lignes pour repérer le début de la ligne suivante. L&#39;ignorance des erreurs ne fonctionne donc que si au moins l&#39;un des deux n&#39;est pas vide.