export const ScalePlanFeatureBadge = ({feature='Эта возможность', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Возможность плана Scale
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'доступны' : 'доступна'} в планах Scale и Enterprise. Чтобы перейти на другой план, откройте страницу планов в Cloud Console.</p>
            </div>
        </div>
    )
}