---
description: 'Un processus enfant sacrifié qui attire le Linux OOM killer avant le
  serveur ClickHouse, laissant à ce dernier le temps de réduire la charge et de survivre.'
sidebar_label: 'OOM canary'
sidebar_position: 60
slug: /operations/settings/oom-canary
title: 'OOM canary'
doc_type: 'référence'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge />

:::note
L’OOM canary est expérimental et désactivé par défaut. Son comportement peut évoluer
d’une version de ClickHouse à l’autre jusqu’à la fin de la validation en production.
:::

<div id="overview">
  ## Vue d&#39;ensemble
</div>

Lorsqu&#39;un hôte ou un cgroup mémoire n&#39;a plus de mémoire disponible, le killer OOM (out-of-memory)
de Linux termine un processus avec `SIGKILL` — généralement le plus gros consommateur, qui,
sur un hôte dédié, est `clickhouse-server` lui-même. Le serveur entier est alors perdu
au lieu d&#39;avoir une chance de se rétablir.

L&#39;OOM canari change la cible tuée en premier. Il exécute un petit processus enfant *sacrificiel*
qui se rend lui-même particulièrement attractif comme cible OOM, afin que le noyau le tue
à la place du serveur. Le serveur détecte alors cette mort, confirme qu&#39;il s&#39;agissait d&#39;un événement OOM
et réduit la pression mémoire pour pouvoir survivre.

L&#39;OOM canari n&#39;augmente aucune limite mémoire et ne remplace pas des
limites correctement définies (voir [Memory overcommit](/fr/operations/settings/memory-overcommit) et
`max_server_memory_usage`). Il constitue une dernière ligne de défense qui échange une petite quantité fixe de mémoire
contre une chance de survivre à un pic de consommation mémoire.

<div id="how-it-works">
  ## Fonctionnement
</div>

Le canari est un processus `clickhouse oom-canary` distinct. Il définit son propre
`oom_score_adj` sur la valeur maximale (`1000`) afin que le noyau le cible en premier, puis
alloue, touche et verrouille avec `mlock` `oom_canary_size` octets (100 Mo par défaut) afin que
sa mémoire résidente soit bien réelle. Il est tué automatiquement si le serveur s&#39;arrête.

Dans le serveur, un thread de supervision surveille le canari (via `pidfd`) et réagit lorsqu&#39;il
meurt :

* Tué par `SIGKILL` **avec** preuve d&#39;OOM au niveau du cgroup → exécuter la réponse OOM, puis
  relancer un nouveau canari.
* Tué **sans** preuve d&#39;OOM (par exemple, un `kill -9` manuel), ou arrêté
  sur un échec transitoire → relance uniquement, sans réponse.
* Échec permanent de l&#39;initialisation, ou arrêt du serveur → le canari se désactive de lui-même.

La preuve d&#39;OOM provient uniquement du compteur `oom_kill` de `memory.events.local` du cgroup v2.
Elle est délibérément locale au cgroup : des compteurs hiérarchiques ou à l&#39;échelle de l&#39;hôte peuvent
être incrémentés par des processus sans rapport et déclencheraient de fausses réponses.

Lors d&#39;un OOM confirmé, la réponse exécute ces étapes indépendantes : consigner un message `FATAL`,
purger les arenas de l&#39;allocateur (jemalloc), annuler dans la mesure du possible toutes les
requêtes en cours d&#39;exécution, annuler tous les merges et mutations, et placer un événement dans
[`system.crash_log`](/fr/operations/system-tables/crash_log). Les log système ne sont pas
vidés de façon synchrone, car forcer des E/S sous pression mémoire peut aggraver la situation.

<div id="requirements">
  ## Prérequis
</div>

* **Linux ≥ 5.3.** Le moniteur gère le canari via `pidfd_open` ; sur les noyaux plus anciens,
  le canari se désactive au démarrage. Cela n&#39;a aucun effet sur les plateformes non Linux.
* **cgroup v2 avec `memory.events.local`** pour la réponse OOM. Sans cela, le
  canari est bien relancé après un `SIGKILL`, mais ne peut pas confirmer un OOM ; la
  réponse n&#39;est donc jamais exécutée (un avertissement est consigné au démarrage).
* **Capacité `mlock` (facultative).** Le verrouillage de la mémoire du canari nécessite
  `CAP_IPC_LOCK` ou une valeur `RLIMIT_MEMLOCK` suffisante ; en cas d&#39;échec, le canari consigne un
  avertissement et sa mémoire peut être évacuée vers le swap, ce qui en fait une moins bonne cible OOM.

:::warning memory.oom.group
Si `memory.oom.group` de cgroup v2 est activé pour le cgroup du serveur, le noyau
tue l&#39;ensemble du cgroup comme une seule unité lors d&#39;un OOM — le serveur meurt avec le
canari et la réponse n&#39;est jamais exécutée. Le canari ne peut pas protéger le serveur dans ce
mode ; un avertissement est consigné au démarrage.
:::

<div id="configuration">
  ## Configuration
</div>

Le canari est contrôlé par les [paramètres du serveur](/fr/operations/server-configuration-parameters/settings),
définis comme éléments de premier niveau de la configuration du serveur et pris en compte au redémarrage.

| Paramètre                            | Défaut               | Description                                                                                                                                                                                                                                             |
| ------------------------------------ | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `oom_canary_enable`                  | `false`              | Active le canari OOM.                                                                                                                                                                                                                                   |
| `oom_canary_size`                    | `104857600` (100 MB) | Nombre d’octets que le canari alloue et touche. Des valeurs plus élevées en font une cible OOM plus probable.                                                                                                                                           |
| `oom_canary_relaunch`                | `true`               | Relance le canari après son arrêt (sauf en cas d’échec permanent de l’initialisation ou d’arrêt du serveur), dans les limites ci-dessous.                                                                                                               |
| `oom_canary_max_rapid_relaunches`    | `10`                 | Nombre maximal de relances *rapides* consécutives avant la désactivation de la relance automatique, afin d’éviter les boucles de redémarrage. Le compteur est réinitialisé dès qu’un canari survit plus longtemps que `oom_canary_max_backoff_seconds`. |
| `oom_canary_initial_backoff_seconds` | `1`                  | Délai initial entre les relances ; il double à chaque fois jusqu’au maximum.                                                                                                                                                                            |
| `oom_canary_max_backoff_seconds`     | `60`                 | Délai maximal entre les relances.                                                                                                                                                                                                                       |

```xml
<clickhouse>
    <oom_canary_enable>1</oom_canary_enable>
    <oom_canary_size>104857600</oom_canary_size>
</clickhouse>
```

<div id="observability">
  ## Observability
</div>

Un OOM confirmé génère une ligne dans
[`system.crash_log`](/fr/operations/system-tables/crash_log) avec `signal = 9` et un
`signal_description` mentionnant `OOM Canary` :

```sql
SELECT event_time, signal, signal_description
FROM system.crash_log
WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'
ORDER BY event_time DESC;
```

Le cycle de vie du processus canari et chaque étape de la réponse aux erreurs OOM sont également consignés dans le journal du serveur.