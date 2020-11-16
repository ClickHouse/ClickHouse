---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "Ayarlardaki k\u0131s\u0131tlamalar"
---

# Ayarlardaki kısıtlamalar {#constraints-on-settings}

Ayarlardaki kısıtlamalar, `profiles` bu bölüm `user.xml` yapılandırma dosyası ve kullanıcıların bazı ayarları değiştirmelerini yasakla `SET` sorgu.
Kısıtlamalar aşağıdaki gibi tanımlanır:

``` xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
    </constraints>
  </user_name>
</profiles>
```

Kullanıcı kısıtlamaları ihlal etmeye çalışırsa, bir istisna atılır ve ayar değiştirilmez.
Desteklenen üç tür kısıtlama vardır: `min`, `max`, `readonly`. Bu `min` ve `max` kısıtlamalar, sayısal bir ayar için üst ve alt sınırları belirtir ve birlikte kullanılabilir. Bu `readonly` kısıtlama, kullanıcının karşılık gelen ayarı hiç değiştiremeyeceğini belirtir.

**Örnek:** İzin vermek `users.xml` hatları içerir:

``` xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

Aşağıdaki sorgular tüm atma istisnaları:

``` sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

``` text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

**Not:** bu `default` profil özel işleme sahiptir: tüm kısıtlamalar için tanımlanan `default` profil varsayılan kısıtlamalar haline gelir, bu nedenle bu kullanıcılar için açıkça geçersiz kılınana kadar tüm kullanıcıları kısıtlarlar.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/settings/constraints_on_settings/) <!--hide-->
