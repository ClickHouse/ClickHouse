const path = require('path');
const jsPath = path.resolve(__dirname, '../../website/src/js');
const scssPath = path.resolve(__dirname, '../../website/src/scss');

console.log(path.resolve(__dirname, 'node_modules/bootstrap', require('bootstrap/package.json').sass));

module.exports = {

	mode: ('development' === process.env.NODE_ENV) && 'development' || 'production',

	...(('development' === process.env.NODE_ENV) && {
		watch: true,
	}),

    entry: [
		path.resolve(scssPath, 'bootstrap.scss'),
		path.resolve(scssPath, 'main.scss'),
		path.resolve(jsPath, 'main.js'),
	],

    output: {
        path: path.resolve(__dirname, '../../website'),
    	filename: 'js/main.js',
    },

	resolve: {
		alias: {
			bootstrap: path.resolve(__dirname, 'node_modules/bootstrap', require('bootstrap/package.json').sass),
		},
	},

	module: {
		rules: [{
			test: /\.js$/,
			exclude: /(node_modules)/,
			use: [{
				loader: 'babel-loader',
				options: {
					presets: ['@babel/preset-env'],
				},
			}],
		}, {
			test: /\.scss$/,
			use: [{
				loader: 'file-loader',
				options: {
					sourceMap: true,
					outputPath: (url, entryPath, context) => {
						if (0 === entryPath.indexOf(scssPath)) {
							const outputFile = entryPath.slice(entryPath.lastIndexOf('/') + 1, -5)
							const outputPath = entryPath.slice(0, entryPath.lastIndexOf('/')).slice(scssPath.length + 1)
							return `./css/${outputPath}/${outputFile}.css`
						}
						return `./css/${url}`
					},
				},
			}, {
				loader: 'postcss-loader',
				options: {
					options: {},
					plugins: () => ([
						require('autoprefixer'),
						('production' === process.env.NODE_ENV) && require('cssnano'),
					].filter(plugin => plugin)),
				}
			}, {
				loader: 'sass-loader',
				options: {
					implementation: require('sass'),
					implementation: require('sass'),
					sourceMap: ('development' === process.env.NODE_ENV),
					sassOptions: {
						importer: require('node-sass-glob-importer')(),
						precision: 10,
					},
				},
			}],
		}],
	},

};
