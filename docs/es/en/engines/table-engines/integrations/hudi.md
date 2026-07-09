---
description: 'Este motor ofrece una integración de solo lectura con tablas Apache Hudi
  existentes en Amazon S3.'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'Motor de tabla Hudi'
doc_type: 'reference'
---

Este motor ofrece una integración de solo lectura con tablas Apache [Hudi](https://hudi.apache.org/) existentes en Amazon S3.

<div id="create-table">
  ## Crear tabla
</div>

Tenga en cuenta que la tabla Hudi ya debe existir en S3; este comando no acepta parámetros DDL para crear una tabla nueva.

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**Parámetros del motor**

* `url` — URL del bucket con la ruta a una tabla Hudi existente.
* `aws_access_key_id`, `aws_secret_access_key` - Credenciales de larga duración para el usuario de la cuenta de [AWS](https://aws.amazon.com/). Puede usarlas para autenticar sus solicitudes. El parámetro es opcional. Si no se especifican credenciales, se usan las del archivo de configuración.
* `extra_credentials` - Opcional. Se usa para pasar un `role_arn` para el acceso basado en roles en ClickHouse Cloud. Consulte [S3 seguro](/es/cloud/data-sources/secure-s3) para ver los pasos de configuración.

Los parámetros del motor se pueden especificar mediante [colecciones con nombre](/es/operations/named-collections.md).

**Ejemplo**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Uso de colecciones con nombre:

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
  ## Véase también
</div>

* [función de tabla Hudi](/es/sql-reference/table-functions/hudi.md)