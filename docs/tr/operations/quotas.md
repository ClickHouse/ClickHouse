---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: Kotalar
---

# Kotalar {#quotas}

Kotalar, belirli bir süre boyunca kaynak kullanımını sınırlamanıza veya kaynak kullanımını izlemenize izin verir.
Kotalar genellikle kullanıcı yapılandırmasında ayarlanır ‘users.xml’.

Sistem ayrıca tek bir sorgunun karmaşıklığını sınırlamak için bir özelliğe sahiptir. Bölümüne bakınız “Restrictions on query complexity”).

Sorgu karmaşıklığı kısıtlamalarının aksine, kotalar:

-   Tek bir sorguyu sınırlamak yerine, belirli bir süre boyunca çalıştırılabilen sorgu kümesine kısıtlamalar yerleştirin.
-   Dağıtılmış sorgu işleme için tüm uzak sunucularda harcanan kaynaklar için hesap.

Bölümüne bakalım ‘users.xml’ kotaları tanımlayan dosya.

``` xml
<!-- Quotas -->
<quotas>
    <!-- Quota name. -->
    <default>
        <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
        <interval>
            <!-- Length of the interval. -->
            <duration>3600</duration>

            <!-- Unlimited. Just collect data for the specified time interval. -->
            <queries>0</queries>
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

Varsayılan olarak, kota, kullanımı sınırlamadan her saat için kaynak tüketimini izler.
Her aralık için hesaplanan kaynak tüketimi, her istekten sonra sunucu günlüğüne çıktıdır.

``` xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <read_rows>500000000000</read_rows>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

İçin ‘statbox’ kota, kısıtlamalar her saat ve her 24 saat (86.400 saniye) için ayarlanır. Aralık saydım, bir uygulama başlangıç zamanı tamir anda tanımlanmış. Başka bir deyişle, 24 saatlik Aralık mutlaka gece yarısı başlamaz.

Aralık sona erdiğinde, toplanan tüm değerler temizlenir. Bir sonraki saat için kota Hesaplaması başlar.

İşte kısıt amountslan theabilecek miktar amountslar:

`queries` – The total number of requests.

`errors` – The number of queries that threw an exception.

`result_rows` – The total number of rows given as a result.

`read_rows` – The total number of source rows read from tables for running the query on all remote servers.

`execution_time` – The total query execution time, in seconds (wall time).

En az bir zaman aralığı için sınır aşılırsa, hangi kısıtlamanın aşıldığı, hangi aralık için ve yeni Aralık başladığında (sorgular yeniden gönderildiğinde) bir metin ile bir istisna atılır.

Kota kullanabilirsiniz “quota key” birden fazla anahtar için kaynakları bağımsız olarak rapor etme özelliği. İşte bunun bir örneği:

``` xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a Yandex.Metrica username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)
    -->
    <keyed />
```

Kota kullanıcılara atanır ‘users’ yapılandırma bölümü. Bölümüne bakınız “Access rights”.

Dağıtılmış sorgu işleme için birikmiş tutarları istekte bulunan sunucuda depolanır. Yani kullanıcı başka bir sunucuya giderse, oradaki kota “start over”.

Sunucu yeniden başlatıldığında, kotalar sıfırlanır.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/quotas/) <!--hide-->
