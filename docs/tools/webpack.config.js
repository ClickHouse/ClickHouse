const path = require('path');

module.exports = {

	mode: ('development' === process.env.NODE_ENV) && 'development' || 'production',

	...(('development' === process.env.NODE_ENV) && {
		watch: true,
	}),

    entry: [
		'../../website/src/scss/main.scss',
	],

    output: {
        path: path.resolve(__dirname, '../../website'),
    	filename: 'js/main.js',
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
					name: 'main.css',
					outputPath: './css',
					sourceMap: true,
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
