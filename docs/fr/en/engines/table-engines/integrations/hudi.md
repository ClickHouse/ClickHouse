---
description: 'Ce moteur offre une intégration en lecture seule aux tables Apache Hudi
  existantes dans Amazon S3.'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'Moteur de table Hudi'
doc_type: 'référence'
---

Ce moteur offre une intégration en lecture seule aux tables Apache [Hudi](https://hudi.apache.org/) existantes dans Amazon S3.

<div id="create-table">
  ## Créer une table
</div>

Notez que la table Hudi doit déjà exister dans S3 ; cette commande n’accepte pas de paramètres DDL pour créer une nouvelle table.

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**Paramètres du moteur**

* `url` — URL du bucket avec le chemin vers une table Hudi existante.
* `aws_access_key_id`, `aws_secret_access_key` - Informations d’identification à long terme pour l’utilisateur du compte [AWS](https://aws.amazon.com/). Vous pouvez les utiliser pour authentifier vos requêtes. Ce paramètre est facultatif. Si les informations d’identification ne sont pas indiquées, celles du fichier de configuration sont utilisées.
* `extra_credentials` - Facultatif. Utilisé pour transmettre un `role_arn` pour l’accès basé sur les rôles dans ClickHouse Cloud. Consultez [S3 sécurisé](/fr/cloud/data-sources/secure-s3) pour connaître les étapes de configuration.

Les paramètres du moteur peuvent être spécifiés à l’aide de [collections nommées](/fr/operations/named-collections.md).

**Exemple**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Utilisation des named collections :

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

<div id="see-also">
  ## Voir aussi
</div>

* [fonction de table Hudi](/fr/sql-reference/table-functions/hudi.md)