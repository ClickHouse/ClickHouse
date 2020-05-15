---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 31
toc_title: Paresseux
---

# Paresseux {#lazy}

Conserve les tables en RAM uniquement `expiration_time_in_seconds` secondes après le dernier accès. Peut être utilisé uniquement avec les tables \* Log.

Il est optimisé pour stocker de nombreuses petites tables \*Log, pour lesquelles il y a un long intervalle de temps entre les accès.

## La création d’une Base De données {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);

[Article Original](https://clickhouse.tech/docs/en/database_engines/lazy/) <!--hide-->
