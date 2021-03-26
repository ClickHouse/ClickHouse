---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 78
toc_title: "Questions G\xE9n\xE9rales"
---

# Questions Générales {#general-questions}

## Pourquoi Ne Pas Utiliser Quelque Chose Comme MapReduce? {#why-not-use-something-like-mapreduce}

Nous pouvons nous référer à des systèmes comme MapReduce en tant que systèmes informatiques distribués dans lesquels l'opération de réduction est basée sur le tri distribué. La solution open-source la plus courante dans cette classe est [Apache Hadoop](http://hadoop.apache.org). Yandex utilise sa solution interne, YT.

Ces systèmes ne sont pas appropriés pour les requêtes en ligne en raison de leur latence élevée. En d'autres termes, ils ne peuvent pas être utilisés comme back-end pour une interface web. Ces types de systèmes ne sont pas utiles pour les mises à jour de données en temps réel. Le tri distribué n'est pas la meilleure façon d'effectuer des opérations de réduction si le résultat de l'opération et tous les résultats intermédiaires (s'il y en a) sont situés dans la RAM d'un seul serveur, ce qui est généralement le cas pour les requêtes en ligne. Dans un tel cas, une table de hachage est un moyen optimal d'effectuer des opérations de réduction. Une approche courante pour optimiser les tâches map-reduce est la pré-agrégation (réduction partielle) à l'aide d'une table de hachage en RAM. L'utilisateur effectue cette optimisation manuellement. Le tri distribué est l'une des principales causes de réduction des performances lors de l'exécution de tâches simples de réduction de la carte.

La plupart des implémentations MapReduce vous permettent d'exécuter du code arbitraire sur un cluster. Mais un langage de requête déclaratif est mieux adapté à OLAP pour exécuter des expériences rapidement. Par exemple, Hadoop a ruche et Cochon. Considérez également Cloudera Impala ou Shark (obsolète) pour Spark, ainsi que Spark SQL, Presto et Apache Drill. Les performances lors de l'exécution de telles tâches sont très sous-optimales par rapport aux systèmes spécialisés, mais une latence relativement élevée rend irréaliste l'utilisation de ces systèmes comme backend pour une interface web.

## Que Faire si j'ai un problème avec les encodages lors de l'utilisation D'Oracle via ODBC? {#oracle-odbc-encodings}

Si vous utilisez Oracle via le pilote ODBC comme source de dictionnaires externes, vous devez définir la valeur correcte pour `NLS_LANG` variable d'environnement dans `/etc/default/clickhouse`. Pour plus d'informations, voir le [FAQ Oracle NLS\_LANG](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**Exemple**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```

## Comment exporter des données de ClickHouse vers un fichier? {#how-to-export-to-file}

### Utilisation de la Clause INTO OUTFILE {#using-into-outfile-clause}

Ajouter un [INTO OUTFILE](../sql-reference/statements/select/into-outfile.md#into-outfile-clause) clause à votre requête.

Exemple:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

Par défaut, ClickHouse utilise [TabSeparated](../interfaces/formats.md#tabseparated) format pour les données de sortie. Pour sélectionner le [format de données](../interfaces/formats.md), utiliser le [FORMAT de la clause](../sql-reference/statements/select/format.md#format-clause).

Exemple:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

### Utilisation d'une Table de moteur de fichiers {#using-a-file-engine-table}

Voir [Fichier](../engines/table-engines/special/file.md).

### Utilisation De La Redirection En Ligne De Commande {#using-command-line-redirection}

``` sql
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

Voir [clickhouse-client](../interfaces/cli.md).

{## [Article Original](https://clickhouse.tech/docs/en/faq/general/) ##}
