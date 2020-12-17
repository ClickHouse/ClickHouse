---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "Tablo Fonksiyonlar\u0131"
toc_priority: 34
toc_title: "Giri\u015F"
---

# Tablo Fonksiyonları {#table-functions}

Tablo işlevleri tabloları oluşturmak için yöntemlerdir.

Tablo işlevlerini kullanabilirsiniz:

-   [FROM](../statements/select/from.md) fıkra ofsı `SELECT` sorgu.

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [Tablo oluştur \<table_function()\>](../statements/create.md#create-table-query) sorgu.

        It's one of the methods of creating a table.

!!! warning "Uyarıcı"
    Eğer tablo işlevlerini kullanamazsınız [allow_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) ayarı devre dışı.

| İşlev                    | Açıklama                                                                                                                    |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| [Dosya](file.md)         | Oluşturur bir [Dosya](../../engines/table-engines/special/file.md)- motor masası.                                           |
| [birleştirmek](merge.md) | Oluşturur bir [Birleştirmek](../../engines/table-engines/special/merge.md)- motor masası.                                   |
| [şiir](numbers.md)       | Tamsayı sayılarla dolu tek bir sütun içeren bir tablo oluşturur.                                                            |
| [uzak](remote.md)        | Oluşturmadan uzak sunuculara erişmenizi sağlar. [Dağılı](../../engines/table-engines/special/distributed.md)- motor masası. |
| [url](url.md)            | Oluşturur bir [Url](../../engines/table-engines/special/url.md)- motor masası.                                              |
| [mysql](mysql.md)        | Oluşturur bir [MySQL](../../engines/table-engines/integrations/mysql.md)- motor masası.                                     |
| [jdbc](jdbc.md)          | Oluşturur bir [JDBC](../../engines/table-engines/integrations/jdbc.md)- motor masası.                                       |
| [odbc](odbc.md)          | Oluşturur bir [ODBC](../../engines/table-engines/integrations/odbc.md)- motor masası.                                       |
| [hdf'ler](hdfs.md)       | Oluşturur bir [HDFS](../../engines/table-engines/integrations/hdfs.md)- motor masası.                                       |

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
