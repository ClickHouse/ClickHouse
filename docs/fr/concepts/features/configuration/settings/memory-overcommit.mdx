---
description: 'Une technique expérimentale visant à définir des limites mémoire plus
  souples pour les requêtes.'
slug: /operations/settings/memory-overcommit
title: 'Memory overcommit'
doc_type: 'référence'
---

Le memory overcommit est une technique expérimentale visant à définir des limites mémoire plus souples pour les requêtes.

Le principe de cette technique est d&#39;introduire des paramètres représentant la quantité de mémoire garantie qu&#39;une requête peut utiliser.
Lorsque le memory overcommit est activé et que la limite mémoire est atteinte, ClickHouse sélectionne la requête la plus en surallocation et tente de libérer de la mémoire en interrompant cette requête.

Lorsque la limite mémoire est atteinte, toute requête attend un certain temps lorsqu&#39;elle tente d&#39;allouer de la mémoire supplémentaire.
Si le délai d&#39;attente est dépassé et que de la mémoire a été libérée, la requête continue son exécution.
Sinon, une exception est levée et la requête est interrompue.

La sélection de la requête à arrêter est effectuée par le traqueur d’overcommit global ou utilisateur, selon la limite mémoire atteinte.
Si le traqueur d’overcommit ne peut pas choisir de requête à arrêter, l&#39;exception MEMORY&#95;LIMIT&#95;EXCEEDED est levée.

<div id="user-overcommit-tracker">
  ## Traqueur d’overcommit utilisateur
</div>

Le traqueur d’overcommit utilisateur identifie la requête présentant le ratio d’overcommit le plus élevé dans la liste des requêtes de l’utilisateur.
Le ratio d’overcommit d’une requête est calculé en divisant le nombre d’octets alloués par la valeur du paramètre `memory_overcommit_ratio_denominator_for_user`.

Si `memory_overcommit_ratio_denominator_for_user` pour la requête est égal à zéro, le traqueur d’overcommit ne sélectionnera pas cette requête.

Le délai d’attente est défini par le paramètre `memory_usage_overcommit_max_wait_microseconds`.

**Exemple**

```sql
SELECT number FROM numbers(1000) GROUP BY number SETTINGS memory_overcommit_ratio_denominator_for_user=4000, memory_usage_overcommit_max_wait_microseconds=500
```

<div id="global-overcommit-tracker">
  ## Traqueur global d’overcommit
</div>

Le traqueur global d’overcommit identifie la requête ayant le ratio d’overcommit le plus élevé dans la liste de toutes les requêtes.
Dans ce cas, le ratio d’overcommit est calculé en divisant le nombre d’octets alloués par la valeur du paramètre `memory_overcommit_ratio_denominator`.

Si `memory_overcommit_ratio_denominator` pour la requête est égal à zéro, le traqueur d’overcommit ne sélectionnera pas cette requête.

Le délai d’attente est défini par le paramètre `memory_usage_overcommit_max_wait_microseconds` dans le fichier de configuration.