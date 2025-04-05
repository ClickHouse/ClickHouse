use blake3::Hasher;
use log::{error, info, trace, warn};
use std::fs;
use std::io::Cursor;
use std::path::Path;

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
struct Config {
    hostname: String,
    user: String,
    password: String,

    source_table: String,
    target_table: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            hostname: "https://build-cache.eu-west-1.aws.clickhouse-staging.com".to_string(),
            user: "reader".to_string(),
            password: "reader".to_string(),

            source_table: "default.build_cache".to_string(),
            target_table: "default.build_cache".to_string(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = xdg::BaseDirectories::with_prefix("chcache")
        .unwrap()
        .place_config_file("config.toml")
        .unwrap();

    let mut env_vars_available = true;
    let required_env_vars = vec!["CH_HOSTNAME", "CH_USER", "CH_PASSWORD"];
    for var in required_env_vars {
        if std::env::var(var).is_ok() {
            continue;
        }

        env_vars_available = false;
        break;
    }

    let config: Config = match (config_path.exists(), env_vars_available) {
        (true, _) => {
            trace!(
                "Loading config file contents from {}",
                config_path.display()
            );

            let config_text = fs::read_to_string(config_path).expect("Missing config file?");
            toml::from_str(&config_text).expect("Unable to load config, is it a valid toml?")
        }
        (_, true) => {
            trace!(
                "Config file not found at {}, trying env vars",
                config_path.display()
            );

            Config {
                hostname: std::env::var("CH_HOSTNAME").unwrap(),
                user: std::env::var("CH_USER").unwrap(),
                password: std::env::var("CH_PASSWORD").unwrap(),

                ..Default::default()
            }
        }
        (false, false) => {
            trace!(
                "Config file not found at {}, and env vars are missing, using defaults",
                config_path.display()
            );
            Config::default()
        }
    };

    env_logger::init();

    compiler_cache_entrypoint(&config).await;

    Ok(())
}

fn assume_base_path(args: &Vec<String>) -> String {
    let cwd: String = match std::env::current_dir() {
        Ok(pathbuf) => pathbuf.into_os_string().into_string().unwrap(),
        _ => {
            panic!("Couldn't get current directory");
        }
    };

    let mut maybe_basepath: Vec<String> = vec![cwd];

    for (i, arg) in args.iter().enumerate() {
        if arg.starts_with("-I") {
            maybe_basepath.push(arg[2..].to_string());
            continue;
        }
        if arg == "-isystem" || arg == "-c" {
            maybe_basepath.push(args[i + 1].to_string());
            continue;
        }
        if arg.starts_with("--gcc-toolchain") {
            maybe_basepath.push(arg[16..].to_string());
            continue;
        }
        if arg.starts_with("--sysroot") {
            maybe_basepath.push(arg[10..].to_string());
            continue;
        }
    }

    let maybe_basepaths_sep_by_slash: Vec<Vec<String>> = maybe_basepath
        .into_iter()
        .map(|x| x.split("/").map(|x| x.to_string()).collect())
        .collect();
    let mut basepath = "".to_string();

    for i in 0..maybe_basepaths_sep_by_slash[0].len() {
        for j in 1..maybe_basepaths_sep_by_slash.len() {
            if maybe_basepaths_sep_by_slash[0][i] != maybe_basepaths_sep_by_slash[j][i] {
                return basepath.trim_end_matches('/').to_string();
            }
        }
        basepath.push_str(&maybe_basepaths_sep_by_slash[0][i]);
        basepath.push_str("/");
    }

    basepath.trim_end_matches('/').to_string()
}

fn compiler_version(compiler: String) -> String {
    trace!("Using compiler: {}", compiler);

    let compiler_version = std::process::Command::new(compiler)
        .arg("-dM")
        .arg("-E")
        .arg("-x")
        .arg("c")
        .arg("/dev/null")
        .output()
        .expect("Failed to execute command");

    let compiler_version = String::from_utf8_lossy(&compiler_version.stdout);

    let compiler_version = compiler_version
        .lines()
        .find(|x| x.starts_with("#define __VERSION__"))
        .unwrap()
        .split_whitespace()
        .skip(2)
        .collect::<Vec<&str>>()
        .join(" ");

    compiler_version.trim_matches('"').to_string()
}

fn get_output_from_args(args: &Vec<String>) -> String {
    let mut target = String::new();
    for (i, arg) in args.iter().enumerate() {
        if arg == "-o" {
            target = args[i + 1].to_string();
            break;
        }
    }
    target
}

fn get_input_from_args(args: &Vec<String>) -> String {
    let mut target = String::new();
    for (i, arg) in args.iter().enumerate() {
        if arg == "-c" {
            target = args[i + 1].to_string();
            break;
        }
    }
    target
}

fn hash_preprocessed_compiler_output(compiler: String, args: &Vec<String>) -> String {
    let mut preprocess_args = vec![
        "-E".to_string(),
        "-P".to_string(),
        "-fminimize-whitespace".to_string(),
    ];
    preprocess_args.extend(args.clone());

    let output_flag_index = preprocess_args.iter().position(|x| x == "-o").unwrap();
    preprocess_args.remove(output_flag_index);
    preprocess_args.remove(output_flag_index);

    let output = std::process::Command::new(compiler)
        .args(preprocess_args)
        .output()
        .expect("Failed to execute command");

    let stdout_output = String::from_utf8_lossy(&output.stdout);
    let stderr_output = String::from_utf8_lossy(&output.stderr);

    if stdout_output.is_empty() {
        panic!("{}", stderr_output);
    }

    let mut hasher = Hasher::new();
    hasher.update(stdout_output.as_bytes());

    let hash = hasher.finalize();
    hash.to_hex().to_string()
}

fn hash_compiler_target(args: &Vec<String>, stripped_args: &Vec<String>) -> String {
    let target: String = get_input_from_args(args);
    let mut hasher = Hasher::new();

    let data = fs::read(&target).expect("Unable to read file");
    hasher.update(&data);

    let target = get_output_from_args(stripped_args);
    hasher.update(&target.as_bytes());

    let hash = hasher.finalize();
    hash.to_hex().to_string()
}

fn path_from_hash(hash: &String) -> String {
    return format!(
        "{}/{}/{}",
        hash.get(0..2).unwrap(),
        hash.get(2..4).unwrap(),
        hash
    );
}

fn get_from_fscache(hash: &String) -> Option<Vec<u8>> {
    let cache_file = xdg::BaseDirectories::with_prefix("chcache")
        .unwrap()
        .get_cache_file(path_from_hash(hash));

    trace!("Cache file: {:?}", cache_file);

    if cache_file.exists() {
        let data = fs::read(cache_file).expect("Unable to read file");
        return Some(data);
    }

    None
}

fn write_to_fscache(hash: &String, data: &Vec<u8>) {
    let cache_file = xdg::BaseDirectories::with_prefix("chcache")
        .unwrap()
        .place_cache_file(path_from_hash(hash))
        .unwrap();

    if cache_file.exists() {
        return;
    }

    let cache_file_dir = cache_file.parent().unwrap();

    if !cache_file_dir.exists() {
        fs::create_dir_all(cache_file_dir).expect("Unable to create directory");
    }

    fs::write(cache_file, data).expect("Unable to write file");
}

async fn load_from_clickhouse(
    config: &Config,
    client: &clickhouse::Client,
    hash: &String,
    compiler_version: &String,
) -> Result<Option<Vec<u8>>, clickhouse::error::Error> {
    let query = format!(
        "SELECT ?fields FROM {} WHERE hash = ? and compiler_version = ? LIMIT 1",
        &config.source_table
    );

    let mut cursor = client
        .query(&query)
        .bind(hash)
        .bind(compiler_version)
        .fetch::<CacheLine>()
        .unwrap();

    while let Some(row) = cursor.next().await? {
        return Ok(Some(row.blob.into()));
    }

    Ok(None)
}

#[derive(Debug, clickhouse::Row, serde::Serialize, serde::Deserialize)]
struct CacheLine {
    #[serde(with = "serde_bytes")]
    blob: Vec<u8>,
    hash: String,
    compiler_version: String,
}

async fn load_to_clickhouse(
    config: &Config,
    client: &clickhouse::Client,
    hash: &String,
    compiler_version: &String,
    data: &Vec<u8>,
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client.insert(&config.target_table).unwrap();

    let row = CacheLine {
        blob: data.clone(),
        hash: hash.clone(),
        compiler_version: compiler_version.clone(),
    };

    insert.write(&row).await.unwrap();
    insert.end().await
}

fn rust_should_cache(args: &[String]) -> bool {
    if args
        .iter()
        .any(|arg| arg == "--version" || arg == "--help" || arg == "--explain" || arg == "-vV")
    {
        return false;
    }

    if args.iter().any(|arg| arg == "--print") {
        return false;
    }

    // crude detection of actual source file
    let has_input = args.iter().any(|arg| arg.ends_with(".rs"));
    if !has_input {
        return false;
    }

    // optionally skip link steps
    if args.iter().any(|arg| arg.contains("emit=link")) {
        return false;
    }

    true
}

// [thevar1able@homebox memchr-2.7.4]$ /home/thevar1able/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rustc --crate-name memchr --edition=2021 /home/thevar1able/.cargo/registry/src/-6df83624996e3d27/memchr-2.7.4/src/lib.rs --error-format=json --json=diagnostic-rendered-ansi,artifacts,future-incompat --diagnostic-width=117 --crate-type lib --emit=dep-info,metadata,link -C embed-bitcode=no -C debuginfo=2 --cfg feature=\"alloc\" --cfg feature=\"std\" --check-cfg "cfg(docsrs,test)" --check-cfg "cfg(feature, values(\"alloc\", \"compiler_builtins\", \"core\", \"default\", \"libc\", \"logging\", \"rustc-dep-of-std\", \"std\", \"use_std\"))" -C metadata=f0ff90587188d79c -C extra-filename=-5282d705ff339125 --out-dir /home/thevar1able/nvmemount/clickhouse/cmake-build-debug/./cargo/build/x86_64-unknown-linux-gnu/debug/deps --target x86_64-unknown-linux-gnu -C linker=/usr/bin/clang -L dependency=/home/thevar1able/nvmemount/clickhouse/cmake-build-debug/./cargo/build/x86_64-unknown-linux-gnu/debug/deps -L dependency=/home/thevar1able/nvmemount/clickhouse/cmake-build-debug/./cargo/build/debug/deps --cap-lints allow -C link-arg=-fuse-ld=lld | head -n1
// {"$message_type":"artifact","artifact":"/home/thevar1able/nvmemount/clickhouse/cmake-build-debug/./cargo/build/x86_64-unknown-linux-gnu/debug/deps/memchr-5282d705ff339125.d","emit":"dep-info"}
// {"$message_type":"artifact","artifact":"/home/thevar1able/nvmemount/clickhouse/cmake-build-debug/./cargo/build/x86_64-unknown-linux-gnu/debug/deps/libmemchr-5282d705ff339125.rmeta","emit":"metadata"}
// {"$message_type":"artifact","artifact":"/home/thevar1able/nvmemount/clickhouse/cmake-build-debug/./cargo/build/x86_64-unknown-linux-gnu/debug/deps/libmemchr-5282d705ff339125.rlib","emit":"link"}

async fn compiler_cache_entrypoint(config: &Config) {
    let compiler: String = std::env::args().nth(1).unwrap();
    let rest_of_args: Vec<String> = std::env::args().skip(2).collect();

    trace!("Compiler: {}", compiler);
    trace!("Args: {:?}", rest_of_args);

    let cwd = std::env::current_dir()
        .unwrap()
        .into_os_string()
        .into_string()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    trace!("Current working directory: {}", cwd);

    if compiler.ends_with("rustc") {
        if !rust_should_cache(&rest_of_args) {
            trace!("Passthrough mode");

            let output = std::process::Command::new(compiler)
                .args(&rest_of_args)
                .output()
                .unwrap();

            if !output.status.success() {
                println!("{}", String::from_utf8_lossy(&output.stdout));
                eprintln!("{}", String::from_utf8_lossy(&output.stderr));
                std::process::exit(output.status.code().unwrap_or(1));
            }

            let output = String::from_utf8_lossy(&output.stdout);
            let output = output
                .lines()
                .filter(|line| !line.is_empty())
                .collect::<Vec<&str>>()
                .join("\n");
            println!("{}", output);

            return;
        }

        let out_dir = rest_of_args
            .iter()
            .position(|x| x == "--out-dir")
            .map(|x| rest_of_args[x + 1].clone())
            .unwrap();

        trace!("Out dir: {:?}", out_dir);

        let mut hasher = Hasher::new();
        rest_of_args.iter().map(|x| x.as_bytes()).for_each(|x| {
            hasher.update(&x);
        });
        let args_hash = hasher.finalize().to_string();

        let compiled_bytes: Vec<u8> = match get_from_fscache(&args_hash) {
            Some(bytes) => {
                info!("Local cache hit");

                let cursor = Cursor::new(bytes.clone());
                let mut archive = tar::Archive::new(cursor);
                archive.unpack(out_dir).expect("Unable to unpack tar");

                bytes
            }
            None => {
                trace!("Cache miss");

                let output = std::process::Command::new(compiler)
                    .args(&rest_of_args)
                    .output()
                    .unwrap();

                if !output.status.success() {
                    println!("{}", String::from_utf8_lossy(&output.stdout));
                    eprintln!("{}", String::from_utf8_lossy(&output.stderr));
                    std::process::exit(output.status.code().unwrap_or(1));
                }

                let files_to_pack = String::from_utf8_lossy(&output.stderr);
                eprintln!("{}", String::from_utf8_lossy(&output.stdout));

                let files_to_pack = files_to_pack
                    .lines()
                    .filter(|line| line.starts_with("{\"$message_type\":\"artifact\""))
                    .collect::<Vec<&str>>();

                let files_to_pack = files_to_pack
                    .iter()
                    .map(|x| {
                        let json: serde_json::Value = serde_json::from_str(x).unwrap();
                        let artifact = json["artifact"].as_str().unwrap();
                        let artifact = artifact.replace("\"", "");
                        artifact
                    })
                    .collect::<Vec<String>>();

                trace!("Files to pack: {:?}", files_to_pack);
                for (key, value) in std::env::vars() {
                    // trace!("Env var: {}: {}", key, value);
                    if key.starts_with("CARGO_") || key == "RUSTFLAGS" || key == "TARGET" {
                        trace!("Maybe interesting env var {}: {}", key, value);
                    }
                }

                let mut buffer = Vec::new();
                let cursor = Cursor::new(&mut buffer);
                let mut archive = tar::Builder::new(cursor);
                for file in files_to_pack {
                    let file = Path::new(&file);
                    let filename = file.strip_prefix(&out_dir).unwrap();
                    let filename = filename.to_str().unwrap();
                    trace!("Packing file: {}", file.display());
                    let mut packed_file = fs::File::open(file).unwrap();
                    archive.append_file(filename, &mut packed_file).unwrap();
                }
                archive.finish().unwrap();
                drop(archive);

                buffer
            }
        };

        write_to_fscache(&args_hash, &compiled_bytes);

        return;
    }

    let assumed_base_path = assume_base_path(&rest_of_args);
    trace!("Assumed base path: {}", assumed_base_path);

    let is_private = Path::new(&assumed_base_path).join("PRIVATE.md").exists();
    trace!("Is private: {}", is_private);

    let stripped_args = rest_of_args
        .iter()
        .map(|x| x.replace(&cwd, "/"))
        .collect::<Vec<String>>();

    let stripped_args = stripped_args
        .iter()
        .map(|x| x.replace(&assumed_base_path, "/"))
        .collect::<Vec<String>>();

    let mut hasher = Hasher::new();
    stripped_args.iter().map(|x| x.as_bytes()).for_each(|x| {
        hasher.update(&x);
    });
    let args_hash = hasher.finalize().to_string();

    let compiler_version = compiler_version(compiler.clone());

    trace!("Compiler version: {}", compiler_version);

    let compiler_target_hash = hash_compiler_target(&rest_of_args, &stripped_args);
    let preprocessed_output_hash =
        hash_preprocessed_compiler_output(compiler.clone(), &rest_of_args);

    trace!("Compiler target hash: {}", compiler_target_hash);
    trace!("Preprocessed output hash: {}", preprocessed_output_hash);
    trace!("Args hash: {}", args_hash);

    hasher.reset();

    hasher.update(compiler_version.as_bytes());
    hasher.update(compiler_target_hash.as_bytes());
    hasher.update(preprocessed_output_hash.as_bytes());
    hasher.update(args_hash.as_bytes());

    let total_hash = hasher.finalize().to_string();
    trace!("Total hash: {}", total_hash);

    let client = clickhouse::Client::default()
        .with_url(&config.hostname)
        .with_user(&config.user)
        .with_password(&config.password)
        .with_compression(clickhouse::Compression::Lz4)
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0");

    let mut did_load_from_cache = false;

    let compiled_bytes: Vec<u8> = match get_from_fscache(&total_hash) {
        Some(bytes) => {
            info!("Local cache hit");
            did_load_from_cache = true;

            fs::write(get_output_from_args(&rest_of_args), &bytes).expect("Unable to write file");

            bytes
        }
        None => {
            trace!("Cache miss");

            let compiled_bytes =
                match load_from_clickhouse(config, &client, &total_hash, &compiler_version).await {
                    Ok(Some(bytes)) => {
                        did_load_from_cache = true;
                        info!("Loaded from ClickHouse");

                        fs::write(get_output_from_args(&rest_of_args), &bytes)
                            .expect("Unable to write file");

                        bytes
                    }
                    Ok(None) | Err(_) => {
                        let output = std::process::Command::new(compiler)
                            .args(&rest_of_args)
                            .output()
                            .unwrap();
                        if !output.status.success() {
                            println!("{}", String::from_utf8_lossy(&output.stdout));
                            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
                            std::process::exit(output.status.code().unwrap_or(1));
                        }
                        fs::read(get_output_from_args(&rest_of_args)).expect("Unable to read file")
                    }
                };

            compiled_bytes
        }
    };

    write_to_fscache(&total_hash, &compiled_bytes);

    let should_upload = {
        let default_config = Config::default();

        !did_load_from_cache && config.user != default_config.user
    };

    if should_upload {
        let mut tries = 3;
        loop {
            let upload_result = load_to_clickhouse(
                config,
                &client,
                &total_hash,
                &compiler_version,
                &compiled_bytes,
            )
            .await;
            if upload_result.is_ok() {
                info!("Uploaded to ClickHouse");
                break;
            }
            warn!("Failed to upload to ClickHouse, retrying...");

            tries -= 1;
            if tries == 0 {
                error!(
                    "Failed to upload to ClickHouse: {}",
                    upload_result.err().unwrap()
                );
                break;
            }
        }
    }
}
