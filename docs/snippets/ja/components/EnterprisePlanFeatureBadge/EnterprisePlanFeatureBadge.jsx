export const EnterprisePlanFeatureBadge = ({feature='この機能', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Enterpriseプランの機能
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'は' : 'は'} Enterpriseプランで利用できます。{support ? `この機能を有効にするには、サポートにお問い合わせください。` : 'アップグレードするには、Cloud Console のプランページにアクセスしてください。'}</p>
            </div>
        </div>
    )
}