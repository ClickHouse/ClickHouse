use log::trace;
use std::fs;

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
pub struct Config {
    pub hostname: String,
    pub user: String,
    pub password: String,

    pub source_table: String,
    pub target_table: String,

    pub use_local_store: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            hostname: "https://build-cache.eu-west-1.aws.clickhouse-staging.com".to_string(),
            user: "reader".to_string(),
            password: "reader".to_string(),

            source_table: "default.build_cache".to_string(),
            target_table: "default.build_cache".to_string(),

            use_local_store: true,
        }
    }
}

impl Config {
    pub fn init() -> Self {
        let config_path = xdg::BaseDirectories::with_prefix("chcache")
            .place_config_file("config.toml")
            .unwrap();

        let mut env_vars_available = true;
        let required_env_vars = vec![
            "CH_HOSTNAME",
            "CH_USER",
            "CH_PASSWORD",
            "CH_USE_LOCAL_CACHE",
        ];
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

                    use_local_store: std::env::var("CH_USE_LOCAL_CACHE")
                        .unwrap()
                        .to_lowercase()
                        .parse::<bool>()
                        .unwrap(),

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

        config
    }
}
