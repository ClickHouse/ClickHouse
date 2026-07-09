---
description: 'Documentation de CREATE VIEW'
sidebar_label: 'VIEW'
sidebar_position: 37
slug: /sql-reference/statements/create/view
title: 'CREATE VIEW'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import DeprecatedBadge from '@theme/badges/DeprecatedBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="create-view">
  # CREATE VIEW
</div>

Crée une nouvelle vue. Les vues peuvent être [normales](#normal-view), [matérialisées](#materialized-view), [matérialisées rafraîchissables](#refreshable-materialized-view) et [de type fenêtre](/fr/sql-reference/statements/create/view#window-view).

<div id="normal-view">
  ## Vue normale
</div>

Syntaxe :

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

Les vues normales ne stockent aucune donnée. Elles se contentent de lire les données d’une autre table à chaque accès. Autrement dit, une vue normale n’est rien d’autre qu’une requête enregistrée. Lors de la lecture d’une vue, cette requête enregistrée est utilisée comme sous-requête dans la clause [FROM](../../../sql-reference/statements/select/from.md).

À titre d’exemple, supposons que vous ayez créé une vue :

```sql
CREATE VIEW view AS SELECT ...
```

et écrit une requête :

```sql
SELECT a, b, c FROM view
```

Cette requête est strictement équivalente à l’utilisation de la sous-requête :

```sql
SELECT a, b, c FROM (SELECT ...)
```

<div id="parameterized-view">
  ## Vue paramétrée
</div>

Les vues paramétrées sont similaires aux vues normales, mais peuvent être créées avec des paramètres qui ne sont pas évalués immédiatement. Ces vues peuvent être utilisées avec des fonctions de table : le nom de la vue est alors utilisé comme nom de fonction, et les valeurs des paramètres comme arguments.

```sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```

L’instruction ci-dessus crée une vue sur la table, qui peut être utilisée comme fonction de table en remplaçant les paramètres, comme indiqué ci-dessous.

```sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

<div id="materialized-view">
  ## Vue matérialisée
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

`OR REPLACE` et `IF NOT EXISTS` s’excluent mutuellement : les combiner constitue une erreur de syntaxe.

<div id="create-or-replace-materialized-view">
  ### CREATE OR REPLACE MATERIALIZED VIEW
</div>

`CREATE OR REPLACE MATERIALIZED VIEW` remplace de façon atomique une vue matérialisée existante ainsi que sa table de stockage interne (le cas échéant). Cette opération nécessite un moteur de base de données `Atomic` ou `Replicated`.

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]name [ON CLUSTER cluster]
[TO [db.]target_table]
[ENGINE = engine]
[POPULATE]
[REFRESH ...]
AS SELECT ...
```

Comportements clés :

* **Sans clause `TO`** : l’ancienne table interne est supprimée et une nouvelle est créée. Les données existantes dans la table interne sont perdues, sauf si `POPULATE` est spécifié.
* **Avec clause `TO`** : seule la définition de la vue est remplacée ; la table cible et ses données ne sont pas affectées.
* Compatible avec `REFRESH`, `ON CLUSTER` et toutes les options de moteur. `POPULATE` est pris en charge uniquement avec les bases de données `Atomic` — il est rejeté avec les bases de données `Replicated` (voir la note sur `POPULATE` ci-dessous).
* Nécessite les privilèges `CREATE VIEW` et `DROP VIEW`.

:::note
`CREATE OR REPLACE MATERIALIZED VIEW` est pris en charge uniquement avec les moteurs de base de données `Atomic` ou `Replicated`. Il n’est pas pris en charge avec le moteur de base de données `Ordinary`.
:::

**Exemples :**

```sql
-- Create a materialized view with an inner table
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, sum(y) AS total FROM src GROUP BY x;

-- Replace with a new definition (old inner table data is lost)
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, count() AS cnt FROM src GROUP BY x;

-- Replace with POPULATE to backfill from existing source data
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    POPULATE
    AS SELECT x FROM src;

-- Replace an inner-table MV with a TO-table MV (target data is preserved)
CREATE OR REPLACE MATERIALIZED VIEW mv TO target
    AS SELECT x FROM src;
```

:::tip
Voici un guide pas à pas sur l’utilisation des [vues matérialisées](/fr/guides/developer/cascading-materialized-views.md).
:::

Les vues matérialisées stockent les données transformées par la requête [SELECT](../../../sql-reference/statements/select/index.md) correspondante.

Lors de la création d’une vue matérialisée sans `TO [db].[table]`, vous devez spécifier `ENGINE` — le moteur de table utilisé pour stocker les données.

Lors de la création d’une vue matérialisée avec `TO [db].[table]`, vous ne pouvez pas utiliser `POPULATE` en même temps.

Une vue matérialisée est implémentée comme suit : lors de l’insertion de données dans la table spécifiée dans `SELECT`, une partie des données insérées est transformée par cette requête `SELECT`, puis le résultat est inséré dans la vue.

:::note
Dans ClickHouse, les vues matérialisées utilisent les **noms de colonnes** plutôt que l’ordre des colonnes lors de l’insertion dans la table de destination. Si certains noms de colonnes ne sont pas présents dans le résultat de la requête `SELECT`, ClickHouse utilise une valeur par défaut, même si la colonne n’est pas [Nullable](../../data-types/nullable.md). Il est recommandé d’ajouter des alias pour chaque colonne lors de l’utilisation de vues matérialisées.

Dans ClickHouse, les vues matérialisées fonctionnent davantage comme des déclencheurs d’insertion. S’il y a une agrégation dans la requête de la vue, elle ne s’applique qu’au lot de données fraîchement insérées. Toute modification des données existantes de la table source (comme update, delete, drop partition, etc.) ne modifie pas la vue matérialisée.

Dans ClickHouse, les vues matérialisées n’ont pas de comportement déterministe en cas d’erreur. Cela signifie que les blocs déjà écrits seront conservés dans la table de destination, mais que tous les blocs suivant l’erreur ne le seront pas.

Par défaut, si l’envoi vers l’une des vues throws, la requête `INSERT` échoue. Rien ne garantit qu’à ce stade le bloc ait déjà atteint la table source : cela dépend du timing du pipeline d’insertion, et non de l’erreur de la vue. Réessayez l’`INSERT` ayant échoué avec la déduplication des insertions (`insert_deduplicate`, `deduplicate_blocks_in_dependent_materialized_views`) pour obtenir une livraison exactly-once vers la table source et toutes les vues dépendantes.

Définir `materialized_views_ignore_errors=true` sur la requête `INSERT` modifie uniquement le signalement des erreurs : chaque erreur de vue est consignée comme avertissement et la requête `INSERT` réussit. La livraison vers la destination de la vue en échec est partielle — les blocs traités avant l&#39;exception sont conservés, et le bloc défaillant ainsi que tous les blocs suivants sont abandonnés pour cette vue. Les vues en aval de cette destination ne voient que les blocs effectivement arrivés ; leur livraison est donc également partielle. Les vues sœurs (et leurs chaînes en aval) qui n&#39;ont pas levé d&#39;exception sont écrites intégralement, et la table source est écrite comme d&#39;habitude. Comme l&#39;`INSERT` est signalé comme réussi, le client ne reçoit aucun signal d&#39;échec et aucune nouvelle tentative automatique n&#39;est déclenchée ; n&#39;utilisez ce paramètre que lorsque les écritures dans la table source ne doivent pas être bloquées par des problèmes côté vue (par exemple, les tables `system.*_log`).

`materialized_views_ignore_errors` vaut `true` par défaut pour les tables `system.*_log`.
:::

Si vous spécifiez `POPULATE`, les données existantes de la table sont insérées dans la vue lors de sa création, comme avec un `CREATE TABLE ... AS SELECT ...`. Sinon, la requête ne contient que les données insérées dans la table après la création de la vue. Nous **ne recommandons pas** d&#39;utiliser `POPULATE`, car les données insérées dans la table pendant la création de la vue n&#39;y seront pas insérées.

:::note
Étant donné que `POPULATE` fonctionne comme `CREATE TABLE ... AS SELECT ...`, il présente certaines limitations :

* Ce n&#39;est pas pris en charge avec une base de données Replicated
* Ce n&#39;est pas pris en charge dans ClickHouse Cloud

À la place, vous pouvez utiliser un `INSERT ... SELECT` distinct.
:::

Une requête `SELECT` peut contenir `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`. Notez que les transformations correspondantes sont effectuées indépendamment sur chaque bloc de données insérées. Par exemple, si `GROUP BY` est défini, les données sont agrégées pendant l&#39;insertion, mais uniquement au sein d&#39;un seul paquet de données insérées. Les données ne seront pas agrégées davantage. L&#39;exception concerne l&#39;utilisation d&#39;un `ENGINE` qui effectue lui-même l&#39;agrégation des données, comme `SummingMergeTree`.

Si la vue matérialisée utilise la construction `TO [db.]name`, vous pouvez `DETACH` la vue, exécuter `ALTER` sur la table cible, puis `ATTACH` la vue précédemment détachée (`DETACH`).

Notez que la vue matérialisée est influencée par le paramètre [optimize&#95;on&#95;insert](/fr/operations/settings/settings#optimize_on_insert). Les données sont fusionnées avant l&#39;insertion dans une vue.

Les vues ressemblent à des tables ordinaires. Par exemple, elles apparaissent dans le résultat de la requête `SHOW TABLES`.

Pour supprimer une vue, utilisez [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). Bien que `DROP TABLE` fonctionne également pour les VIEWs.

<div id="sql_security">
  ## SQL security
</div>

`DEFINER` et `SQL SECURITY` vous permettent de spécifier quel utilisateur ClickHouse utiliser lors de l’exécution de la requête sous-jacente de la vue.
`SQL SECURITY` admet trois valeurs : `DEFINER`, `INVOKER` ou `NONE`. Vous pouvez spécifier n’importe quel utilisateur existant ou `CURRENT_USER` dans la clause `DEFINER`.

Le tableau suivant indique quels droits sont requis pour quel utilisateur afin de sélectionner des données depuis la vue.
Notez que, quelle que soit l’option SQL security, il est dans tous les cas nécessaire de disposer de `GRANT SELECT ON <view>` pour pouvoir la lire.

| Option SQL security | Vue                                                                              | Vue matérialisée                                                                                                                 |
| ------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `DEFINER alice`     | `alice` doit disposer du privilège `SELECT` sur la table source de la vue.       | `alice` doit disposer du privilège `SELECT` sur la table source de la vue et du privilège `INSERT` sur la table cible de la vue. |
| `INVOKER`           | L’utilisateur doit disposer du privilège `SELECT` sur la table source de la vue. | `SQL SECURITY INVOKER` ne peut pas être spécifié pour les vues matérialisées.                                                    |
| `NONE`              | -                                                                                | -                                                                                                                                |

:::note
`SQL SECURITY NONE` est une option déconseillée. Tout utilisateur ayant le droit de créer des vues avec `SQL SECURITY NONE` pourra exécuter n’importe quelle requête arbitraire.
Il est donc nécessaire de disposer de `GRANT ALLOW SQL SECURITY NONE TO <user>` pour créer une vue avec cette option.
:::

Si `DEFINER`/`SQL SECURITY` ne sont pas spécifiés, les valeurs par défaut sont utilisées :

* `SQL SECURITY` : `INVOKER` pour les vues normales et `DEFINER` pour les vues matérialisées ([configurable dans les paramètres](../../../operations/settings/settings.md#default_normal_view_sql_security))
* `DEFINER` : `CURRENT_USER` ([configurable dans les paramètres](../../../operations/settings/settings.md#default_view_definer))

Si une vue est attachée sans que `DEFINER`/`SQL SECURITY` soient spécifiés, la valeur par défaut est `SQL SECURITY NONE` pour la vue matérialisée et `SQL SECURITY INVOKER` pour la vue normale.

Pour modifier SQL security d’une vue existante, utilisez

```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

<div id="examples">
  ### Exemples
</div>

```sql
CREATE VIEW test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE VIEW test_view
SQL SECURITY INVOKER
AS SELECT ...
```

<div id="live-view">
  ## Live View
</div>

<DeprecatedBadge />

Cette fonctionnalité est obsolète et sera supprimée ultérieurement.

Pour plus de simplicité, l’ancienne documentation est disponible [ici](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

<div id="refreshable-materialized-view">
  ## Vue matérialisée rafraîchissable
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
REFRESH [EVERY|AFTER interval [OFFSET interval]]
[RANDOMIZE FOR interval]
[DEPENDS ON [db.]name [, [db.]name [, ...]]]
[SETTINGS name = value [, name = value [, ...]]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine]
[EMPTY]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

où `interval` est une séquence d’intervalles simples :

```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

La clause `REFRESH` doit spécifier au moins un de ces éléments : `EVERY`, `AFTER` ou `DEPENDS ON`. Un `REFRESH` seul (sans aucun d’eux) est refusé. `REFRESH DEPENDS ON ...` sans `EVERY`/`AFTER` est une forme abrégée de `REFRESH AFTER 0 SECOND DEPENDS ON ...` ; voir [Dépendances de rafraîchissement](#refresh-dependencies) ci-dessous.

Exécute périodiquement la requête correspondante et stocke son résultat dans une table.

* Si `APPEND` est spécifié, chaque rafraîchissement insère des lignes dans la table sans supprimer les lignes existantes. L’insertion n’est pas atomique, comme avec une requête `INSERT INTO ... SELECT` classique.
* Sinon, chaque rafraîchissement remplace atomiquement le contenu précédent de la table.

Différences par rapport aux vues matérialisées classiques non rafraîchissables :

* Pas de trigger d’insertion. Lorsque de nouvelles données sont insérées dans la table spécifiée dans `SELECT`, elles ne sont *pas* automatiquement envoyées vers la vue matérialisée rafraîchissable. À la place, l’insertion des données n’a lieu que lors des rafraîchissements périodiques ou manuels.
* Aucune restriction sur la requête `SELECT`. Les fonctions de table (par ex. `url()`), les vues, `UNION` et `JOIN` sont tous autorisés.

:::note
Les paramètres de la partie `REFRESH ... SETTINGS` de la requête sont des paramètres de rafraîchissement (par ex. `refresh_retries`), distincts des paramètres classiques (par ex. `max_threads`). Les paramètres classiques peuvent être spécifiés avec `SETTINGS` à la fin de la requête.
:::

<div id="refresh-schedule">
  ### Planification du rafraîchissement
</div>

Exemples de planifications du rafraîchissement :

```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax error, OFFSET is not allowed with AFTER
REFRESH EVERY 1 WEEK 2 DAYS -- every 9 days, not on any particular day of the week or month;
                            -- specifically, when day number (since 1969-12-29) is divisible by 9
REFRESH EVERY 5 MONTHS -- every 5 months, different months each year (as 12 is not divisible by 5);
                       -- specifically, when month number (since 1970-01) is divisible by 5
```

`RANDOMIZE FOR` décale aléatoirement l’heure de chaque rafraîchissement, par exemple :

```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

Un seul rafraîchissement peut être en cours à la fois pour une vue donnée. Par exemple, si le rafraîchissement d&#39;une vue avec `REFRESH EVERY 1 MINUTE` prend 2 minutes, elle ne sera en pratique rafraîchie que toutes les 2 minutes. Si elle devient ensuite plus rapide et commence à se rafraîchir en 10 secondes, elle reviendra à un rafraîchissement toutes les minutes. (En particulier, elle ne se rafraîchira pas toutes les 10 secondes pour rattraper un arriéré de rafraîchissements manqués - il n&#39;existe pas d&#39;arriéré de ce type.)

En règle générale, le premier rafraîchissement démarre immédiatement après la création de la vue matérialisée : le temps écoulé depuis le dernier rafraîchissement est infini, donc toute planification indique qu&#39;il faut rafraîchir immédiatement. Si `EMPTY` est spécifié, ce rafraîchissement initial est ignoré, et le premier rafraîchissement a lieu à la prochaine heure planifiée ; par exemple, pour `EVERY 1 HOUR`, le premier rafraîchissement aura lieu à la fin de l&#39;heure en cours.

<div id="in-replicated-db">
  ### Dans une base de données Replicated
</div>

Si la vue matérialisée rafraîchissable se trouve dans une [base de données Replicated](../../../engines/database-engines/replicated.md), les répliques se coordonnent entre elles afin qu&#39;une seule réplique effectue le rafraîchissement à chaque échéance planifiée. Le moteur de table [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) est requis pour que toutes les répliques voient les données produites par le rafraîchissement.

En mode `APPEND`, la coordination peut être désactivée à l&#39;aide de `SETTINGS all_replicas = 1`. Les répliques effectuent alors les rafraîchissements indépendamment les unes des autres. Dans ce cas, ReplicatedMergeTree n&#39;est pas requis.

En mode non-`APPEND`, seul le rafraîchissement coordonné est pris en charge. Pour un rafraîchissement non coordonné, utilisez la base de données `Atomic` et la requête `CREATE ... ON CLUSTER` pour créer des vues matérialisées rafraîchissables sur toutes les répliques.

La coordination s&#39;effectue via Keeper. Le chemin du znode est déterminé par le paramètre de serveur [default&#95;replica&#95;path](../../../operations/server-configuration-parameters/settings.md#default_replica_path).

<div id="refresh-dependencies">
  ### Dépendances de rafraîchissement
</div>

`DEPENDS ON` synchronise les rafraîchissements de différentes tables :

```sql
CREATE MATERIALIZED VIEW dependent REFRESH EVERY 1 HOUR DEPENDS ON dependency [...]
```

Le rafraîchissement de la vue dépendante ne démarrera qu’une fois le rafraîchissement de toutes les vues dont elle dépend terminé.

Pour rafraîchir immédiatement après le rafraîchissement d’une autre vue :

```sql
CREATE MATERIALIZED VIEW dependent REFRESH AFTER 0 SECOND DEPENDS ON dependency [...]
```

Ou, de façon équivalente :

```sql
CREATE MATERIALIZED VIEW dependent REFRESH DEPENDS ON dependency [...]
```

:::note
`DEPENDS ON` fonctionne uniquement entre des vues matérialisées actualisables. En particulier, si la vue de dépendance utilise `TO <table>`, veillez à utiliser le nom de la vue plutôt que celui de la table. Si la liste `DEPENDS ON` contient une table classique, une vue non actualisable ou une faute de frappe, la vue ne sera jamais actualisée et son état sera `MissingDependencies` dans `system.view_refreshes`. Les dépendances peuvent être modifiées ou supprimées avec `ALTER`, voir [Modification des paramètres d’actualisation](#changing-refresh-parameters).
:::

<div id="using-depends-on-for-consistent-propagation-latency">
  #### Utilisation de DEPENDS ON pour une latence de propagation cohérente
</div>

Si les deux vues utilisent `REFRESH EVERY` avec la même période, la dépendance s&#39;applique à chaque créneau temporel.

Par exemple, supposons que les vues X et Y utilisent toutes deux `REFRESH EVERY 1 HOUR`, et que Y lit dans la table de sortie de X. Sans dépendances, Y verrait généralement les données de X provenant du rafraîchissement de l&#39;heure précédente. Avec `DEPENDS ON X`, le rafraîchissement de Y à 11:00 ne démarrera qu&#39;une fois le rafraîchissement de X à 11:00 terminé.

```text
           10:00            11:00            12:00
           │                │                │
  X:        [run]┐           [run]┐           [run]┐
                 │                │                │
  Y:             └►[run]          └►[run]          └►[run]
```

La dépendance et l’élément dépendant peuvent chacun ignorer des créneaux temporels si les actualisations durent plus longtemps que la période d’actualisation. Rien ne garantit que l’actualisation de l’élément dépendant s’exécute exactement une fois pour chaque actualisation de la dépendance.

```text
           10:00          11:00          12:00          13:00
           │              │              │              |
  X:        [run]┐         [run]┐         [run]┐         [run]┐
                 │              └────┐    (Y skips 12:00)     └───┐
  Y:             └►[10:00 ru------un]└►[11:00 ru---------------un]└►[13:00 run]
```

<div id="using-depends-on-for-batched-stream-processing">
  #### Utilisation de DEPENDS ON pour le traitement de flux par lots
</div>

Si `REFRESH EVERY` n’est pas utilisé, la vue dépendante X se rafraîchit si toutes ses dépendances se sont rafraîchies au moins une fois depuis le dernier rafraîchissement de X. `REFRESH AFTER T` ajoute un délai : la vue dépendante commencera à se rafraîchir un temps T après qu’une dépendance a terminé un rafraîchissement.

Les dépendances circulaires sont autorisées et utiles. Considérez ce graphe de vues matérialisées rafraîchissables :

1. X prend un lot de lignes depuis un flux et les place dans une table.
2. Ensuite, Y et Z lisent tous deux dans cette table, effectuent des agrégations différentes et ajoutent les résultats à d’autres tables.
3. Une fois le lot entièrement traité, X prend le lot suivant, et le cycle se répète.

```text
            source
               │
               ▼
          ┌─────────┐
     ┌───►│    X    │◄───┐
     │    └──┬───┬──┘    │
  DEPENDS    │   │    DEPENDS
    ON       ▼   ▼      ON
     │      ┌─┐ ┌─┐      │
     └──────┤Y│ │Z├──────┘
            └─┘ └─┘
```

Exemple complet :

```sql
CREATE TABLE current_batch (t UInt64, v Int64) ENGINE ReplicatedMergeTree ORDER BY t;
CREATE TABLE batch_log (max_t UInt64, n Int64, v_sum Int64, processed_at DateTime64) ENGINE ReplicatedMergeTree ORDER BY max_t;
CREATE TABLE stats (h UInt64, n UInt64) ENGINE ReplicatedSummingMergeTree ORDER BY h;

-- (system.numbers stands in for a data source with monotonically increasing timestamps or sequence numbers)
CREATE MATERIALIZED VIEW current_batch_v REFRESH EVERY 10 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS SELECT number as t, number * 10 as v FROM system.numbers WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 100;

CREATE MATERIALIZED VIEW batch_log_v REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS SELECT max(t) as max_t, count() as n, sum(v) as v_sum, now64() as processed_at FROM current_batch;

CREATE MATERIALIZED VIEW stats_v REFRESH DEPENDS ON current_batch_v APPEND TO stats AS SELECT cityHash64(v) % 20 as h, count() as n FROM current_batch GROUP BY h;

-- Must trigger initial refresh manually.
SYSTEM REFRESH VIEW current_batch_v;
```

Des chaînes plus longues fonctionnent également.

Cela ne fonctionne bien que lorsque la coordination du rafraîchissement est activée, c’est-à-dire lorsque les vues se trouvent dans une base de données Replicated ou Shared. Sans coordination, le redémarrage du serveur interrompt le cycle, ce qui nécessite un `SYSTEM REFRESH VIEW` manuel après chaque redémarrage, au lieu d’une seule fois après la création des vues.

<div id="refresh-settings">
  ### Paramètres de rafraîchissement
</div>

Paramètres de rafraîchissement disponibles :

* `refresh_retries` - Nombre de nouvelles tentatives si la requête de rafraîchissement échoue avec une exception. Si toutes les tentatives échouent, le système passe à l’heure de rafraîchissement planifiée suivante. 0 signifie aucune nouvelle tentative, -1 signifie un nombre infini de tentatives. Par défaut : 2.
* `refresh_retry_initial_backoff_ms` - Délai avant la première nouvelle tentative, si `refresh_retries` n’est pas égal à zéro. Chaque tentative suivante double ce délai, jusqu’à `refresh_retry_max_backoff_ms`. Par défaut : 100 ms.
* `refresh_retry_max_backoff_ms` - Limite de la croissance exponentielle du délai entre les tentatives de rafraîchissement. Par défaut : 60000 ms (1 minute).
* `all_replicas` - Dans une [base de données Replicated](../../../engines/database-engines/replicated.md) avec `APPEND`, contrôle si toutes les répliques se rafraîchissent indépendamment ou si une seule réplique se rafraîchit à chaque heure planifiée. Ne peut pas être modifié après la création de la vue. Par défaut : `false`.

<div id="changing-refresh-parameters">
  ### Modification des paramètres de rafraîchissement
</div>

Les paramètres de rafraîchissement d’une vue matérialisée rafraîchissable existante se modifient avec [`ALTER TABLE ... MODIFY REFRESH`](../alter/view.md#alter-table--modify-refresh-statement):

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

L’ordonnancement (`EVERY` ou `AFTER`) est obligatoire : l’instruction remplace toujours *tous* les paramètres de rafraîchissement — l’ordonnancement, `RANDOMIZE FOR`, `DEPENDS ON` et les paramètres de rafraîchissement — par ceux qui sont spécifiés. Tout élément omis est réinitialisé à sa valeur par défaut (pour les paramètres) ou supprimé (pour les dépendances et la randomisation).

:::note

* Pour modifier uniquement les paramètres de rafraîchissement (par ex. `refresh_retries`), répétez l’ordonnancement existant :

  ```sql
  ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 5;
  ```

* `ALTER TABLE ... MODIFY SETTING refresh_retries = ...` n’est pas pris en charge pour les vues matérialisées ; vous devez passer par `MODIFY REFRESH`.

* L’ajout ou la suppression de `APPEND` n’est pas pris en charge.

* Le paramètre `all_replicas` ne peut pas être modifié après la création.
  :::

Exemples :

```sql
-- Change the schedule, drop existing settings and dependencies.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change the schedule and tune retry behavior.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Keep the dependency while changing the period.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

<div id="other-operations">
  ### Autres opérations
</div>

L&#39;état de toutes les vues matérialisées rafraîchissables est disponible dans la table [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md). Elle contient notamment la progression du rafraîchissement (si elle est en cours), l&#39;heure du dernier et du prochain rafraîchissement, ainsi que le message d&#39;exception si un rafraîchissement a échoué.

Pour arrêter, démarrer, déclencher ou annuler manuellement des rafraîchissements, utilisez [`SYSTEM STOP|START|REFRESH|WAIT|CANCEL VIEW`](../system.md#managing-refreshable-materialized-views).

Pour attendre la fin d&#39;un rafraîchissement, utilisez [`SYSTEM WAIT VIEW`](../system.md#wait-view). C&#39;est particulièrement utile pour attendre le rafraîchissement initial après la création d&#39;une vue.

:::note
Fait amusant : la requête de rafraîchissement est autorisée à lire à partir de la vue en cours de rafraîchissement et voit la version des données antérieure au rafraîchissement. Cela signifie que vous pouvez implémenter le jeu de la vie de Conway : https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
:::

<div id="window-view">
  ## Window View
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
Il s’agit d’une fonctionnalité expérimentale qui pourrait évoluer de manière incompatible avec les versions précédentes dans les prochaines versions. Activez l’utilisation des window views et de la requête `WATCH` à l’aide du paramètre [allow&#95;experimental&#95;window&#95;view](/fr/operations/settings/settings#allow_experimental_window_view). Saisissez la commande `set allow_experimental_window_view = 1`.
:::

```sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE]
AS SELECT ...
GROUP BY time_window_function
[COMMENT 'comment']
```

Une window view peut agréger les données par fenêtre temporelle et produire les résultats lorsque la fenêtre est prête à être émise. Elle stocke les résultats d’agrégation partiels dans une table interne (ou spécifiée) afin de réduire la latence, et peut envoyer le résultat du traitement vers une table spécifiée ou envoyer des notifications à l’aide de la requête WATCH.

La création d’une window view est similaire à celle d’une `MATERIALIZED VIEW`. Une window view a besoin d’un moteur de stockage interne pour stocker les données intermédiaires. Le stockage interne peut être spécifié à l’aide de la clause `INNER ENGINE` ; la window view utilisera `AggregatingMergeTree` comme moteur interne par défaut.

Lors de la création d’une window view sans `TO [db].[table]`, vous devez spécifier `ENGINE` – le moteur de table utilisé pour stocker les données.

<div id="time-window-functions">
  ### Fonctions de fenêtre temporelle
</div>

Les [fonctions de fenêtre temporelle](../../functions/time-window-functions.md) servent à obtenir les bornes inférieure et supérieure de la fenêtre pour les enregistrements. La window view doit être utilisée avec une fonction de fenêtre temporelle.

<div id="time-attributes">
  ### ATTRIBUTS TEMPORELS
</div>

Window view prend en charge le traitement en **temps de traitement** et en **temps d&#39;événement**.

Le **temps de traitement** permet à Window view de produire des résultats en fonction de l&#39;heure de la machine locale et est utilisé par défaut. C&#39;est la notion du temps la plus simple, mais elle ne garantit pas le déterminisme. L&#39;attribut de temps de traitement peut être défini en affectant à `time_attr` de la fonction de fenêtre temporelle une colonne de table, ou en utilisant la fonction `now()`. La requête suivante crée une Window view avec le temps de traitement.

```sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

Le **temps d&#39;événement** correspond au moment où chaque événement individuel s&#39;est produit sur l&#39;appareil qui l&#39;a généré. Ce temps est généralement intégré aux enregistrements lors de leur création. Le traitement en temps d&#39;événement permet d&#39;obtenir des résultats cohérents même en cas d&#39;événements arrivant dans le désordre ou en retard. La window view prend en charge le traitement en temps d&#39;événement à l&#39;aide de la syntaxe `WATERMARK`.

La window view propose trois stratégies de watermark :

* `STRICTLY_ASCENDING` : émet un watermark correspondant à l&#39;horodatage maximal observé jusqu&#39;à présent. Les lignes dont l&#39;horodatage est inférieur à l&#39;horodatage maximal ne sont pas considérées comme tardives.
* `ASCENDING` : émet un watermark correspondant à l&#39;horodatage maximal observé jusqu&#39;à présent moins 1. Les lignes dont l&#39;horodatage est égal à l&#39;horodatage maximal ou lui est inférieur ne sont pas considérées comme tardives.
* `BOUNDED` : WATERMARK=INTERVAL. Émet des watermarks correspondant à l&#39;horodatage maximal observé moins le délai spécifié.

Les requêtes suivantes montrent comment créer une window view avec `WATERMARK` :

```sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

Par défaut, la fenêtre émet son résultat à l’arrivée du watermark, et les éléments arrivés après celui-ci sont ignorés. Window view prend en charge le traitement des événements tardifs en définissant `ALLOWED_LATENESS=INTERVAL`. Voici un exemple de gestion des retards :

```sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

Notez que les éléments émis lors d’un déclenchement tardif doivent être traités comme des résultats mis à jour d’un calcul précédent. Au lieu de se déclencher à la fin des fenêtres, la window view se déclenche immédiatement à l’arrivée de l’événement tardif. Il en résulte donc plusieurs sorties pour la même fenêtre. Les utilisateurs doivent tenir compte de ces résultats dupliqués ou les dédupliquer.

Vous pouvez modifier la requête `SELECT` spécifiée dans la window view à l’aide de l’instruction `ALTER TABLE ... MODIFY QUERY`. La structure de données résultant de la nouvelle requête `SELECT` doit être identique à celle de la requête `SELECT` d’origine, avec ou sans la clause `TO [db.]name`. Notez que les données de la fenêtre en cours seront perdues, car l’état intermédiaire ne peut pas être réutilisé.

<div id="monitoring-new-windows">
  ### Surveiller les nouvelles fenêtres
</div>

La window view permet d&#39;utiliser la requête [WATCH](../../../sql-reference/statements/watch.md) pour surveiller les changements, ou la syntaxe `TO` pour écrire les résultats dans une table.

```sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

Un `LIMIT` peut être spécifié pour définir le nombre de mises à jour à recevoir avant la fin de la requête. La clause `EVENTS` permet d’obtenir une forme abrégée de la requête `WATCH` dans laquelle, au lieu du résultat de la requête, vous n’obtiendrez que le dernier watermark de la requête.

<div id="settings-1">
  ### Paramètres
</div>

* `window_view_clean_interval` : Intervalle de nettoyage de la window view, en secondes, afin de libérer les données obsolètes. Le système conserve les fenêtres qui n’ont pas encore été entièrement déclenchées selon l’heure système ou la configuration `WATERMARK`, et supprime les autres données.
* `window_view_heartbeat_interval` : Intervalle de pulsation, en secondes, indiquant que la requête watch est toujours active.
* `wait_for_window_view_fire_signal_timeout` : Délai d’attente du signal de déclenchement de la window view lors du traitement en temps d’événement.

<div id="example">
  ### Exemple
</div>

Supposons que nous devions compter le nombre de logs de clics par tranche de 10 secondes dans une table de logs nommée `data`, dont la structure est la suivante :

```sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

Tout d’abord, nous créons une window view avec une fenêtre tumbling sur un intervalle de 10 secondes :

```sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

Ensuite, nous utilisons la requête `WATCH` pour récupérer les résultats.

```sql
WATCH wv
```

Lorsque des logs sont insérés dans la table `data`,

```sql
INSERT INTO data VALUES(1,now())
```

La requête `WATCH` devrait afficher les résultats comme suit :

```text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

Sinon, nous pouvons également envoyer la sortie vers une autre table à l’aide de la syntaxe `TO`.

```sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

D&#39;autres exemples se trouvent dans les tests stateful de ClickHouse (ils y portent le nom `*window_view*`).

<div id="window-view-usage">
  ### Utilisation des window views
</div>

La window view est utile dans les scénarios suivants :

* **Supervision** : agréger et calculer les métriques des logs au fil du temps, puis écrire les résultats dans une table cible. Le tableau de bord peut utiliser la table cible comme table source.
* **Analyse** : agréger et prétraiter automatiquement les données dans la fenêtre temporelle. Cela peut être utile lors de l&#39;analyse d&#39;un grand nombre de logs. Le prétraitement évite les calculs répétés dans plusieurs requêtes et réduit la latence des requêtes.

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Travailler avec des données de séries temporelles dans ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* Blog : [Créer une solution d’observabilité avec ClickHouse - Partie 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

<div id="temporary-views">
  ## Vues temporaires
</div>

ClickHouse prend en charge les **vues temporaires** avec les caractéristiques suivantes (comme pour les tables temporaires, le cas échéant) :

* **Durée de vie de la session**
  Une vue temporaire n’existe que pendant la session en cours. Elle est supprimée automatiquement à la fin de la session.

* **Pas de base de données**
  Vous **ne pouvez pas** associer une vue temporaire à un nom de base de données. Elle existe en dehors des bases de données (dans l’espace de noms de la session).

* **Non répliqué / pas de ON CLUSTER**
  Les objets temporaires sont locaux à la session et **ne peuvent pas** être créés avec `ON CLUSTER`.

* **Résolution des noms**
  Si un objet temporaire (table ou vue) porte le même nom qu’un objet persistant et qu’une requête référence ce nom **sans** base de données, c’est l’objet **temporaire** qui est utilisé.

* **Objet logique (pas de stockage)**
  Une vue temporaire stocke uniquement son texte `SELECT` (en utilisant en interne le moteur `View`). Elle ne conserve pas les données et n’accepte pas les `INSERT`.

* **Clause ENGINE**
  Il n’est **pas** nécessaire de spécifier `ENGINE` ; si `ENGINE = View` est fourni, il est ignoré et traité comme la même vue logique.

* **Sécurité / privilèges**
  La création d’une vue temporaire nécessite le privilège `CREATE TEMPORARY VIEW`, implicitement accordé par `CREATE VIEW`.

* **SHOW CREATE**
  Utilisez `SHOW CREATE TEMPORARY VIEW view_name;` pour afficher le DDL d’une vue temporaire.

<div id="temporary-views-syntax">
  ### Syntaxe
</div>

```sql
CREATE TEMPORARY VIEW [IF NOT EXISTS] view_name AS <select_query>
```

`OR REPLACE` n’est **pas** pris en charge pour les vues temporaires (comme pour les tables temporaires). Si vous devez « remplacer » une vue temporaire, supprimez-la et recréez-la.

<div id="examples">
  ### Exemples
</div>

Créez une table source temporaire et une vue temporaire à partir de celle-ci :

```sql
CREATE TEMPORARY TABLE t_src (id UInt32, val String);
INSERT INTO t_src VALUES (1, 'a'), (2, 'b');

CREATE TEMPORARY VIEW tview AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview ORDER BY id;
```

Afficher le DDL :

```sql
SHOW CREATE TEMPORARY VIEW tview;
```

Supprimez-la :

```sql
DROP TEMPORARY VIEW IF EXISTS tview;  -- temporary views are dropped with TEMPORARY TABLE syntax
```

<div id="temporary-views-limitations">
  ### Interdits / limitations
</div>

* `CREATE OR REPLACE TEMPORARY VIEW ...` → **non autorisé** (utilisez `DROP` + `CREATE`).
* `CREATE TEMPORARY MATERIALIZED VIEW ...` / `WINDOW VIEW` → **non autorisé**.
* `CREATE TEMPORARY VIEW db.view AS ...` → **non autorisé** (pas de qualificatif de base de données).
* `CREATE TEMPORARY VIEW view ON CLUSTER 'name' AS ...` → **non autorisé** (les objets temporaires sont locaux à la session).
* `POPULATE`, `REFRESH`, `TO [db.table]`, les moteurs internes et toutes les clauses spécifiques aux MV → **non applicables** aux vues temporaires.

<div id="temporary-views-distributed-notes">
  ### Remarques sur les requêtes distribuées
</div>

Une **vue** temporaire n’est qu’une définition ; il n’y a donc aucune donnée à transférer. Si votre vue temporaire fait référence à des **tables** temporaires (par exemple `Memory`), leurs données peuvent être envoyées à des serveurs distants lors de l’exécution distribuée de requêtes, de la même manière que pour les tables temporaires.

<div id="temporary-views-distributed-example">
  #### Exemple
</div>

```sql
-- A session-scoped, in-memory table
CREATE TEMPORARY TABLE temp_ids (id UInt64) ENGINE = Memory;

INSERT INTO temp_ids VALUES (1), (5), (42);

-- A session-scoped view over the temp table (purely logical)
CREATE TEMPORARY VIEW v_ids AS
SELECT id FROM temp_ids;

-- Replace 'test' with your cluster name.
-- GLOBAL JOIN forces ClickHouse to *ship* the small join-side (temp_ids via v_ids)
-- to every remote server that executes the left side.
SELECT count()
FROM cluster('test', system.numbers) AS n
GLOBAL ANY INNER JOIN v_ids USING (id)
WHERE n.number < 100;

```