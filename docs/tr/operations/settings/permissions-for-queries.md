---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "Sorgular i\xE7in izinler"
---

# Sorgular için izinler {#permissions_for_queries}

Clickhouse'daki sorgular birkaç türe ayrılabilir:

1.  Veri sorgularını oku: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  Veri sorgularını yaz: `INSERT`, `OPTIMIZE`.
3.  Ayarları değiştir sorgu: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) sorgular: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

Aşağıdaki ayarlar, kullanıcı izinlerini sorgu Türüne göre düzenler:

-   [readonly](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
-   [allow\_ddl](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` herhangi bir ayar ile yapılabilir.

## readonly {#settings_readonly}

Veri okuma, veri yazma ve ayar sorgularını değiştirme izinlerini kısıtlar.

Sorguların türlere nasıl ayrıldığını görün [üzerinde](#permissions_for_queries).

Olası değerler:

-   0 — All queries are allowed.
-   1 — Only read data queries are allowed.
-   2 — Read data and change settings queries are allowed.

Sonra ayarı `readonly = 1`, kullanıcı değiştir canemez `readonly` ve `allow_ddl` geçerli oturumda ayarlar.

Kullanırken `GET` metho thed in the [HTTP arayüzü](../../interfaces/http.md), `readonly = 1` otomatik olarak ayarlanır. Değiştirmek için veri kullanın `POST` yöntem.

Ayar `readonly = 1` kullanıcının tüm ayarları değiştirmesini yasaklayın. Kullanıcıyı yasaklamanın bir yolu var
sadece belirli ayarları değiştirmekten, ayrıntılar için bkz [ayarlardaki kısıtlamalar](constraints-on-settings.md).

Varsayılan değer: 0

## allow\_ddl {#settings_allow_ddl}

İzin verir veya reddeder [DDL](https://en.wikipedia.org/wiki/Data_definition_language) sorgular.

Sorguların türlere nasıl ayrıldığını görün [üzerinde](#permissions_for_queries).

Olası değerler:

-   0 — DDL queries are not allowed.
-   1 — DDL queries are allowed.

Yürüt canemezsiniz `SET allow_ddl = 1` eğer `allow_ddl = 0` geçerli oturum için.

Varsayılan değer: 1

[Orijinal makale](https://clickhouse.tech/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
