---
description: 'Guide de configuration et d’utilisation des scripts SQL de démarrage dans ClickHouse pour
  la création automatique du schéma et les migrations'
sidebar_label: 'Scripts de démarrage'
slug: /operations/startup-scripts
title: 'Scripts de démarrage'
doc_type: 'guide'
---

ClickHouse peut exécuter des requêtes SQL arbitraires depuis la configuration du serveur lors du démarrage. Cela peut être utile pour les migrations ou la création automatique du schéma.

```xml
<clickhouse>
    <startup_scripts>
        <throw_on_error>false</throw_on_error>
        <scripts>
            <query>CREATE ROLE OR REPLACE test_role</query>
        </scripts>
        <scripts>
            <query>CREATE TABLE TestTable (id UInt64) ENGINE=TinyLog</query>
            <condition>SELECT 1;</condition>
        </scripts>
        <scripts>
            <query>CREATE DICTIONARY test_dict (...) SOURCE(CLICKHOUSE(...))</query>
            <user>default</user>
        </scripts>
    </startup_scripts>
</clickhouse>
```

ClickHouse exécute toutes les requêtes de `startup_scripts` séquentiellement, dans l’ordre spécifié. Si l’une des requêtes échoue, l’exécution des requêtes suivantes ne sera pas interrompue. Cependant, si `throw_on_error` est défini sur `true`,
le serveur ne démarrera pas si une erreur se produit pendant l’exécution du script.

Vous pouvez spécifier une requête conditionnelle dans la configuration. Dans ce cas, la requête correspondante s’exécute uniquement lorsque la requête de condition renvoie la valeur `1` ou `true`.

:::note
Si la requête de condition renvoie une valeur autre que `1` ou `true`, le résultat sera interprété comme `false`, et la requête correspondante ne sera pas exécutée.
:::