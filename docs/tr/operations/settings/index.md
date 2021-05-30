---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Ayarlar
toc_priority: 55
toc_title: "Giri\u015F"
---

# Ayarlar {#session-settings-intro}

Belgelerin bu bölümünde açıklanan tüm ayarları yapmanın birden çok yolu vardır.

Ayarlar katmanlar halinde yapılandırılır, böylece sonraki her katman önceki ayarları yeniden tanımlar.

Öncelik sırasına göre ayarları yapılandırma yolları:

-   Ayarlar `users.xml` sunucu yapılandırma dosyası.

    Eleman setında Set `<profiles>`.

-   Oturum ayarları.

    Göndermek `SET setting=value` etkileşimli modda ClickHouse konsol istemcisinden.
    Benzer şekilde, http protokolünde ClickHouse oturumlarını kullanabilirsiniz. Bunu yapmak için şunları belirtmeniz gerekir `session_id` HTTP parametresi.

-   Sorgu ayarları.

    -   ClickHouse konsol istemcisini etkileşimli olmayan modda başlatırken, başlangıç parametresini ayarlayın `--setting=value`.
    -   HTTP API'sini kullanırken, CGI parametrelerini geçirin (`URL?setting_1=value&setting_2=value...`).

Yalnızca sunucu yapılandırma dosyasında yapılabilecek ayarlar bu bölümde yer almaz.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/settings/) <!--hide-->
