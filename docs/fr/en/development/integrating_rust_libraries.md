---
description: 'Guide d’intégration de bibliothèques Rust dans ClickHouse'
sidebar_label: 'Bibliothèques Rust'
slug: /development/integrating_rust_libraries
title: 'Intégration de bibliothèques Rust'
doc_type: 'guide'
---

L’intégration de bibliothèques Rust sera illustrée à partir de l’intégration de la fonction de hachage BLAKE3.

La première étape consiste à ajouter la bibliothèque au dossier /rust. Pour ce faire, vous devez créer un projet Rust vide et inclure la bibliothèque requise dans Cargo.toml. Il est également nécessaire de configurer la compilation de cette nouvelle bibliothèque en tant que bibliothèque statique en ajoutant `crate-type = ["staticlib"]` à Cargo.toml.

Ensuite, vous devez lier la bibliothèque à CMake à l’aide de la bibliothèque Corrosion. La première étape consiste à ajouter le dossier de la bibliothèque dans le fichier CMakeLists.txt du dossier /rust. Après cela, vous devez ajouter le fichier CMakeLists.txt au répertoire de la bibliothèque. Dans ce fichier, vous devez appeler la fonction d’importation de Corrosion. Les lignes suivantes ont été utilisées pour importer BLAKE3 :

```CMake
corrosion_import_crate(MANIFEST_PATH Cargo.toml NO_STD)

target_include_directories(_ch_rust_blake3 INTERFACE include)
add_library(ch_rust::blake3 ALIAS _ch_rust_blake3)
```

Ainsi, nous créerons une cible CMake correcte à l’aide de Corrosion, puis nous la renommerons avec un nom plus pratique. Notez que le nom `_ch_rust_blake3` provient de Cargo.toml, où il est utilisé comme nom de projet (`name = "_ch_rust_blake3"`).

Comme les data types Rust ne sont pas compatibles avec les data types C/C++, nous utiliserons notre projet de bibliothèque vide pour créer des méthodes d’adaptation permettant de convertir les données reçues depuis C/C++, d’appeler les methods de la bibliothèque, puis d’effectuer la conversion inverse pour les données de sortie. Par exemple, cette méthode a été écrite pour BLAKE3 :

```rust
#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim(
    begin: *const c_char,
    _size: u32,
    out_char_data: *mut u8,
```

```rust
#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim(
    begin: *const c_char,
    _size: u32,
    out_char_data: *mut u8,
) -> *mut c_char {
    if begin.is_null() {
        let err_str = CString::new("input was a null pointer").unwrap();
        return err_str.into_raw();
    }
    let mut hasher = blake3::Hasher::new();
    let input_bytes = CStr::from_ptr(begin);
    let input_res = input_bytes.to_bytes();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();
    reader.fill(std::slice::from_raw_parts_mut(out_char_data, blake3::OUT_LEN));
    std::ptr::null_mut()
}
```

Cette méthode reçoit en entrée une chaîne compatible C, sa taille et un pointeur vers la chaîne de sortie. Elle convertit ensuite les entrées compatibles C en types utilisés par les véritables méthodes de la bibliothèque, puis les appelle. Après cela, elle doit reconvertir les sorties des méthodes de la bibliothèque en type compatible C. Dans ce cas particulier, la bibliothèque prenait en charge l&#39;écriture directe dans le pointeur via la méthode fill(), donc la conversion n&#39;était pas nécessaire. Le principal conseil ici est de créer moins de méthodes, afin d&#39;avoir moins de conversions à effectuer à chaque appel de méthode et de ne pas générer trop de surcharge.

Il convient de noter que l&#39;attribut `#[no_mangle]` et `extern "C"` sont obligatoires pour toutes ces méthodes. Sans eux, il ne sera pas possible d&#39;effectuer une compilation correcte compatible C/C++. De plus, ils sont nécessaires pour l&#39;étape suivante de l&#39;intégration.

Après avoir écrit le code des méthodes d’adaptation, nous devons préparer le fichier d’en-tête de la bibliothèque. Cela peut être fait manuellement, ou vous pouvez utiliser la bibliothèque cbindgen pour une génération automatique. En cas d&#39;utilisation de cbindgen, vous devrez écrire un script de build build.rs et inclure cbindgen comme build-dependency.

Un exemple de script de build pouvant générer automatiquement un fichier d’en-tête :

```rust
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let package_name = env::var("CARGO_PKG_NAME").unwrap();
    let output_file = ("include/".to_owned() + &format!("{}.h", package_name)).to_string();

    match cbindgen::generate(&crate_dir) {
        Ok(header) => {
            header.write_to_file(&output_file);
        }
        Err(err) => {
            panic!("{}", err)
        }
    }
```

De plus, vous devez utiliser l’attribut #[no&#95;mangle] et `extern "C"` pour tout attribut compatible avec C. Sans cela, la bibliothèque risque d’être compilée incorrectement et cbindgen ne pourra pas lancer l’autogénération du fichier d’en-tête.

Après toutes ces étapes, vous pouvez tester votre bibliothèque dans un petit projet afin de repérer d&#39;éventuels problèmes de compatibilité ou de génération d&#39;en-têtes. Si des problèmes surviennent lors de la génération des en-têtes, vous pouvez essayer de la configurer à l&#39;aide du fichier cbindgen.toml (vous trouverez un Template ici : [https://github.com/eqrion/cbindgen/blob/master/template.toml](https://github.com/eqrion/cbindgen/blob/master/template.toml)).

Il convient également de signaler le problème rencontré lors de l&#39;intégration de BLAKE3 :
MemorySanitizer peut provoquer des faux positifs, car il ne peut pas déterminer si certaines variables Rust sont initialisées ou non. Ce problème a été résolu en écrivant une fonction avec une définition plus explicite pour certaines variables, même si cette implémentation est plus lente et n&#39;est utilisée que pour corriger les builds de MemorySanitizer.