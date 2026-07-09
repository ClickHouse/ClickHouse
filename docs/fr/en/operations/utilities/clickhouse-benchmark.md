---
description: 'Documentation de clickhouse-benchmark '
sidebar_label: 'clickhouse-benchmark'
sidebar_position: 61
slug: /operations/utilities/clickhouse-benchmark
title: 'clickhouse-benchmark'
doc_type: 'reference'
---

Se connecte à un serveur ClickHouse et envoie de façon répétée les requêtes spécifiées.

**Syntaxe**

```bash
$ clickhouse-benchmark --query ["single query"] [keys]
```

ou

```bash
$ echo "single query" | clickhouse-benchmark [keys]
```

ou

```bash
$ clickhouse-benchmark [keys] <<< "single query"
```

Si vous souhaitez envoyer une série de requêtes, créez un fichier texte et placez chaque requête sur une ligne distincte dans ce fichier. Par exemple :

```sql
SELECT * FROM system.numbers LIMIT 10000000;
SELECT 1;
```

Passez ensuite ce fichier à l’entrée standard de `clickhouse-benchmark` :

```bash
clickhouse-benchmark [keys] < queries_file;
```

<div id="clickhouse-benchmark-command-line-options">
  ## Options de ligne de commande
</div>

* `--query=QUERY` — Requête à exécuter. Si ce paramètre n’est pas fourni, `clickhouse-benchmark` lit les requêtes depuis l’entrée standard.
* `--query_id=ID` — ID de requête.
* `--query_id_prefix=ID_PREFIX` — Préfixe de l’ID de requête.
* `--queries-format=FORMAT` — Format des requêtes lues depuis l’entrée standard. Valeurs possibles : `tsv` (par défaut, une requête par ligne avec tabulations échappées) et `script` (interprète l’entrée comme un script contenant plusieurs requêtes séparées par des points-virgules). Limitation de `script` : les requêtes `INSERT ... FORMAT` doivent tenir sur une seule ligne.
* `-c N`, `--concurrency=N` — Nombre de requêtes que `clickhouse-benchmark` envoie simultanément. Valeur par défaut : 1.
* `-C N`, `--max_concurrency=N` — Augmente progressivement le nombre de requêtes parallèles jusqu’à la valeur indiquée, en produisant un rapport pour chaque niveau de concurrence.
* `--precise` — Active des rapports précis par intervalle avec des métriques pondérées.
* `-d N`, `--delay=N` — Intervalle, en secondes, entre les rapports intermédiaires (pour désactiver les rapports, définir 0). Valeur par défaut : 1.
* `-h HOST`, `--host=HOST` — Hôte du serveur. Valeur par défaut : `localhost`. En [mode de comparaison](#clickhouse-benchmark-comparison-mode), vous pouvez utiliser plusieurs options `-h`.
* `-i N`, `--iterations=N` — Nombre total de requêtes. Valeur par défaut : 0 (répéter indéfiniment).
* `-r`, `--randomize` — Exécute les requêtes dans un ordre aléatoire s’il y a plusieurs requêtes en entrée.
* `-s`, `--secure` — Utilise une connexion `TLS`.
* `-t N`, `--timelimit=N` — Limite de temps en secondes. `clickhouse-benchmark` cesse d’envoyer des requêtes lorsque la limite de temps indiquée est atteinte. Valeur par défaut : 0 (limite de temps désactivée).
* `--port=N` — Port du serveur. Valeur par défaut : 9000. En [mode de comparaison](#clickhouse-benchmark-comparison-mode), vous pouvez utiliser plusieurs options `--port`.
* `--confidence=N` — Niveau de confiance pour le test t. Valeurs possibles : 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99,5%). Valeur par défaut : 5. En [mode de comparaison](#clickhouse-benchmark-comparison-mode), `clickhouse-benchmark` exécute le [test t de Student pour deux échantillons indépendants](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) afin de déterminer si les deux distributions ne diffèrent pas au niveau de confiance sélectionné.
* `--cumulative` — Affiche les données cumulées au lieu des données par intervalle.
* `--database=DATABASE_NAME` — Nom de la base de données ClickHouse. Valeur par défaut : `default`.
* `--user=USERNAME` — Nom d’utilisateur ClickHouse. Valeur par défaut : `default`.
* `--password=PSWD` — Mot de passe de l’utilisateur ClickHouse. Valeur par défaut : chaîne vide.
* `--stacktrace` — Affiche les traces de pile. Lorsque cette option est définie, `clickhouse-benchmark` affiche les traces de pile des exceptions.
* `--stage=WORD` — Étape du traitement de la requête sur le serveur. ClickHouse arrête le traitement de la requête et renvoie une réponse à `clickhouse-benchmark` à l’étape indiquée. Valeurs possibles : `complete`, `fetch_columns`, `with_mergeable_state`. Valeur par défaut : `complete`.
* `--roundrobin` — Au lieu de comparer les requêtes entre différents `--host`/`--port`, choisit simplement un `--host`/`--port` aléatoire pour chaque requête et lui envoie la requête.
* `--reconnect=N` — Contrôle le comportement de reconnexion. Valeurs possibles : 0 (ne jamais se reconnecter), 1 (se reconnecter pour chaque requête) ou N (se reconnecter après chaque groupe de N requêtes). Valeur par défaut : 0.
* `--max-consecutive-errors=N` — Nombre d’erreurs consécutives autorisées. Valeur par défaut : 0.
* `--ignore-error`,`--continue_on_errors` — Continue le test même si des requêtes ont échoué.
* `--client-side-time` — Affiche le temps incluant la communication réseau au lieu du temps côté serveur ; notez que, pour les versions du serveur antérieures à 22.8, le temps affiché est toujours le temps côté client.
* `--proto-caps` — Active/désactive le découpage en fragments dans le transfert de données. Choix possibles (séparés par des virgules) : `chunked_optional`, `notchunked`, `notchunked_optional`, `send_chunked`, `send_chunked_optional`, `send_notchunked`, `send_notchunked_optional`, `recv_chunked`, `recv_chunked_optional`, `recv_notchunked`, `recv_notchunked_optional`. Valeur par défaut : `notchunked`.
* `--help` — Affiche le message d’aide.
* `--verbose` — Augmente le niveau de détail du message d’aide.

Si vous souhaitez appliquer certains [paramètres](/fr/operations/settings/overview) aux requêtes, indiquez-les sous la forme d’une clé `--<session setting name>= SETTING_VALUE`. Par exemple, `--max_memory_usage=1048576`.

<div id="clickhouse-benchmark-environment-variable-options">
  ## Options des variables d&#39;environnement
</div>

Le nom d&#39;utilisateur, le mot de passe et l&#39;hôte peuvent être définis via les variables d&#39;environnement `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` et `CLICKHOUSE_HOST`.
Les arguments de ligne de commande `--user`, `--password` ou `--host` prévalent sur les variables d&#39;environnement.

<div id="clickhouse-benchmark-output">
  ## Sortie
</div>

Par défaut, `clickhouse-benchmark` produit un rapport pour chaque intervalle `--delay`.

Exemple de rapport :

```text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

Dans le rapport, vous pouvez trouver :

* Le nombre de requêtes dans le champ `Queries executed:`.

* Une chaîne d’état contenant (dans l’ordre) :

  * L’endpoint du serveur ClickHouse.
  * Le nombre de requêtes traitées.
  * QPS : le nombre de requêtes que le serveur exécute par seconde sur une période spécifiée dans l’argument `--delay`.
  * RPS : le nombre de lignes que le serveur lit par seconde sur une période spécifiée dans l’argument `--delay`.
  * MiB/s : le nombre de mébioctets que le serveur lit par seconde sur une période spécifiée dans l’argument `--delay`.
  * result RPS : le nombre de lignes que le serveur renvoie dans le résultat d’une requête par seconde sur une période spécifiée dans l’argument `--delay`.
  * result MiB/s. Le nombre de mébioctets que le serveur renvoie dans le résultat d’une requête par seconde sur une période spécifiée dans l’argument `--delay`.

* Les percentiles du temps d’exécution des requêtes.

<div id="clickhouse-benchmark-comparison-mode">
  ## Mode de comparaison
</div>

`clickhouse-benchmark` peut comparer les performances de deux serveurs ClickHouse en fonctionnement.

Pour utiliser le mode de comparaison, indiquez les adresses des deux serveurs à l’aide de deux paires d’options `--host`, `--port`. Les options sont associées par leur position dans la liste des arguments : le premier `--host` est associé au premier `--port`, et ainsi de suite. `clickhouse-benchmark` établit des connexions aux deux serveurs, puis envoie les requêtes. Chaque requête est adressée à un serveur choisi aléatoirement. Les résultats sont affichés dans un tableau.

<div id="clickhouse-benchmark-example">
  ## Exemple
</div>

```bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark --host=localhost --port=9001 --host=localhost --port=9000 -i 10
```

```text
Loaded 1 queries.

Queries executed: 5.

localhost:9001, queries 2, QPS: 3.764, RPS: 75446929.370, MiB/s: 575.614, result RPS: 37639659.982, result MiB/s: 287.168.
localhost:9000, queries 3, QPS: 3.815, RPS: 76466659.385, MiB/s: 583.394, result RPS: 38148392.297, result MiB/s: 291.049.

0.000%          0.258 sec.      0.250 sec.
10.000%         0.258 sec.      0.250 sec.
20.000%         0.258 sec.      0.250 sec.
30.000%         0.258 sec.      0.267 sec.
40.000%         0.258 sec.      0.267 sec.
50.000%         0.273 sec.      0.267 sec.
60.000%         0.273 sec.      0.267 sec.
70.000%         0.273 sec.      0.267 sec.
80.000%         0.273 sec.      0.269 sec.
90.000%         0.273 sec.      0.269 sec.
95.000%         0.273 sec.      0.269 sec.
99.000%         0.273 sec.      0.269 sec.
99.900%         0.273 sec.      0.269 sec.
99.990%         0.273 sec.      0.269 sec.

No difference proven at 99.5% confidence
```